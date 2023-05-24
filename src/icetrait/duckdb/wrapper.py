import duckdb

from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import Table as IcebergTable
import pyarrow.dataset as ds
import pyarrow.parquet as pq

import sqlparse

from substrait.gen.proto.plan_pb2 import Plan as SubstraitPlan

from icetrait.iceberg.process import IcebergFileDownloader
from icetrait.substrait.visitor import (ExtractTableVisitor,
                                        NamedTableUpdateVisitor,
                                        RelUpdateVisitor,
                                        SubstraitPlanEditor,
                                        extract_rel_from_plan,
                                        visit_and_update
                                        )

class DuckdbSubstrait:
    
    def __init__(self, catalog_name:str, local_path:str, duckdb_schema:str, sql_query:str, setup_func):
        ## initialize duckdb
        self._con = setup_func()
        if not isinstance(self._con, duckdb.DuckDBPyConnection):
            raise ValueError(f"setup_func must return a duckdb.DuckDBPyConnection instance, instead it returned {type(self._con)}")
        self._con.install_extension("substrait")
        self._con.load_extension("substrait")
        self._con.install_extension("httpfs")
        self._con.load_extension("httpfs")
        ## initialize parameters
        self._sql_query = sql_query
        plan_proto_bytes = self._con.get_substrait(sql_query).fetchone()[0]
        self._plan = plan_proto_bytes # saves original plan
        self._substrait_plan = SubstraitPlan() # get used to make changes
        self._substrait_plan.ParseFromString(self._plan)
        self._catalog_name = catalog_name
        self._duckdb_schema = duckdb_schema
        self._table_name = None
        self._files = None
        self._formats = None
        self._updated_plan = None # get used to get the final output
        self._local_path = local_path
        
        ## pyiceberg_parameters
        
        # used in DataScan(selected_fields=*)
        self._selected_fields, self._selected_fields_aliases = self.extract_fields(self._sql_query)

    @property
    def plan(self):
        return self._substrait_plan
    
    @property
    def table_name(self):
        if not self._table_name:
            self.set_table_name_from_plan()
        return self._table_name
    
    @property
    def table_name_with_schema(self):
        return f"{self._duckdb_schema}.{self.table_name}"
    
    def set_table_name_from_plan(self):
        extract_visitor = ExtractTableVisitor()
        rel = extract_rel_from_plan(self._plan)
        visit_and_update(rel, extract_visitor)
        self._table_name = extract_visitor.table_names[0]

    def update_named_table_with_schema(self):
        editor = SubstraitPlanEditor(self._plan)
        named_table_update_visitor = NamedTableUpdateVisitor(self.table_name_with_schema)
        visit_and_update(editor.rel, named_table_update_visitor)
        self._updated_plan = editor.plan
        
    def extract_alias(self, statement):
        name = None
        alias = None
        if "as" in statement:
            splits = statement.split("as")
            name = splits[0].strip()
            alias = splits[1].strip()
        elif "AS" in statement:
            splits = statement.split("AS")
            name = splits[0].strip()
            alias = splits[1].strip()
        else:
            name = statement
            alias = ""
        return name, alias

    def extract_fields(self, query):
        # Parse the SQL query
        parsed = sqlparse.parse(query)

        # The parsed result is a list of statements. We assume that there's only one
        # statement in your query, so we get the first one.
        stmt = parsed[0]

        # Check if the statement is a SELECT statement
        if stmt.get_type() != 'SELECT':
            raise ValueError('Query must be a SELECT statement')

        # Iterate over the tokens in the parsed statement
        fields = []
        aliases = []
        for token in stmt.tokens:
            # Tokens can be various types. The field names are identified by type sqlparse.sql.Identifier
            # In case of multiple fields being selected, they are of type sqlparse.sql.IdentifierList
            if token.value.upper() == 'FROM':
                return fields, aliases
            if isinstance(token, sqlparse.sql.Identifier):
                name, alias = self.extract_alias(str(token))
                fields.append(name)
                aliases.append(alias)
            elif isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    name, alias = self.extract_alias(str(identifier))
                    fields.append(name)
                    aliases.append(alias)
            elif token.value == "*":
                return ["*"], [""]

    @DeprecationWarning
    def _get_columns_in_sql_statement(self):
        parsed = sqlparse.parse(self._sql_query)
        statement = parsed[0]
        # extracting column names
        col_names = ["*"]

        for token in statement.tokens:
            if isinstance(token, sqlparse.sql.IdentifierList):
                str_token = str(token)
                col_names = str_token.split(",")
                
        names = []
        aliases = []

        for col_name in col_names:
            if "as" in col_name:
                splits = col_name.split("as")
                names.append(splits[0].strip())
                aliases.append(splits[1].strip())
            elif "AS" in col_name:
                splits = col_name.split("AS")
                names.append(splits[0].strip())
                aliases.append(splits[1].strip())
            else: 
                names.append(col_name.strip())
        return names, aliases
        
    def extract_info_from_input_plan(self):
        """_summary_
        TODO: 
        From the existing plan we can gather important information. 
        It contains the required output_names which are an accurate
        representation of ouput columns of the data. 
        
        The names here and the names in the file are different. 
        1. First compare original plan root_rel.names vs projected_schema and file_schema
        
            FROM THE ORIGINAL SQL Statement, we should be able to extract the required columns
            
            ```python
                import sqlparse

                sql_statement = "SELECT A, B, C FROM TABLE;"
                parsed = sqlparse.parse(sql_statement)

                # Assuming the first statement is the one we want
                statement = parsed[0]

                # Extract column names
                col_names = None

                for token in statement.tokens:
                    if isinstance(token, sqlparse.sql.IdentifierList):
                        str_token = str(token)
                        col_names = str_token.split(",")
                        
                trimmed_cols = []
                for col_name in col_names:
                    trimmed_cols.append(col_name.strip())
            ```
            use the `trimmed_cols` in `IcebergFileDownloader.download(selected_fields=trimmed_cols)
            within the function in the sc = self.table.scan(selected_fields=selected_fields)
            
            This would only load the required information.
            
            If there are filters, we should be able to use the same logic and push them down as 
            Substrait filter expressions. Need to think about this too. 
            
        """
        pass

    def update_with_local_file_paths(self):
        # this method would update the passed Substrait plan
        # with s3 urls to local files 
        # Issue: https://github.com/duckdb/duckdb/discussions/7252
        downloader = IcebergFileDownloader(catalog=self._catalog_name, table=self.table_name_with_schema, local_path=self._local_path)
        self._files, self._formats, base_schema, root_rel_names, current_schema = downloader.download(selected_fields=self._selected_fields)
        output_names = []
        if self._selected_fields == ["*"]:
            output_names = root_rel_names
        else: 
            output_names = self._selected_fields
        print("self._selected_fields")
        print(self._selected_fields)
        print("output_names")
        print(output_names)
        projection_fields = []
        print(current_schema)
        # fields = current_schema.fields
        # for relative_id, field in enumerate(fields):
        #     if field.name in base_schema.names:
        #         print("Field Found : ", relative_id, field.name)
        #         #  TODO: you have to put the relative index according to the base_schema (full schema?)
        #         projection_fields.append(relative_id)
        def find_index(base_schema, value):
            for idx, name in enumerate(base_schema.names):
                if name == value:
                    return idx
                
        def find_index_in_iceberg_schema(schema, field):
            for field in schema.fields():
                print(field.field_id)
            
        base_schema_names = base_schema.names       
        for item in output_names:
            if item in base_schema_names:
                index = find_index(base_schema=base_schema, value=item)
                projection_fields.append(index)
        

        update_visitor = RelUpdateVisitor(files=self._files, formats=self._formats, base_schema=base_schema, current_schema=current_schema, output_names=output_names, projection_fields=projection_fields)
        editor = SubstraitPlanEditor(self._updated_plan.SerializeToString())
        visit_and_update(editor.rel, update_visitor)
        # TODO: if selected_fields = [*], we need to make sure we update the names accordingly
        # assert len(root_rel_names) == len(existing_root_rel_names)
        # update output names
        # if editor.plan.relations:
        #     relations = editor.plan.relations
        #     if relations:
        #         if relations[0].HasField("root"):
        #             rel_root = relations[0].root
        #             rel_root.names[:] = output_names
        self._updated_plan = editor.plan

    @property
    def updated_plan(self):
        return self._updated_plan
    
    def execute(self):
        # run the updated Substrait plan with DuckDb
        proto_bytes = self._updated_plan.SerializeToString()
        query_result = self._con.from_substrait(proto=proto_bytes)
        return query_result


def run_query(plan: SubstraitPlan, catalog_name:str, local_path:str, duckdb_schema:str):
    def setup_func():
        return duckdb.connect()
    wrapper = DuckdbSubstrait(plan=plan, catalog_name=catalog_name, local_path=local_path, duckdb_schema=duckdb_schema, setup_func=setup_func)
    wrapper.update_named_table_with_schema()
    wrapper.update_with_local_file_paths()
    return wrapper.execute()
