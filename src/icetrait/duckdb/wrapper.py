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
        self._selected_fields, self._selected_fields_aliases = self._get_columns_in_sql_statement()

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
        self._files, self._formats, base_schema, root_rel_names = downloader.download(selected_fields=self._selected_fields)
        output_names = []
        if self._selected_fields == ["*"]:
            output_names = root_rel_names
        else: 
            output_names = self._selected_fields
        print("self._selected_fields")
        print(self._selected_fields)
        print("output_names")
        print(output_names)
        update_visitor = RelUpdateVisitor(files=self._files, formats=self._formats, base_schema=base_schema, output_names=output_names)
        editor = SubstraitPlanEditor(self._updated_plan.SerializeToString())
        visit_and_update(editor.rel, update_visitor)
        # update output names
        if editor.plan.relations:
            relations = editor.plan.relations
            if relations:
                if relations[0].HasField("root"):
                    rel_root = relations[0].root
                    rel_root.names[:] = output_names
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
