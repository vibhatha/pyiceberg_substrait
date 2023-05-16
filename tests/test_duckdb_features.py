import shutil, tempfile
from os import path
from typing import List

import duckdb
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from icetrait.substrait.visitor import RelVisitor, NamedTableUpdateVisitor, SubstraitPlanEditor, visit_and_update, RelUpdateVisitor, IcebergSubstraitRelVisitor
from icetrait.duckdb.wrapper import DuckdbSubstrait

from icetrait.iceberg.process import arrow_table_to_substrait

class RelValidateVisitor(RelVisitor):
        def __init__(self, files, formats, table_name):
            self._files = files
            self._formats = formats
            self._table_name = table_name

        def visit_aggregate(self, rel):
            pass

        def visit_cross(self, rel):
            pass

        def visit_fetch(self, rel):
            pass

        def visit_filter(self, rel):
            pass

        def visit_join(self, rel):
            pass

        def visit_hashjoin(self, rel):
            pass

        def visit_merge(self, rel):
            pass

        def visit_project(self, rel):
            pass

        def visit_read(self, read_rel):
            if self._files and self._formats:
                local_files = read_rel.local_files
                for id, file in enumerate(local_files.items):
                    assert self._files[id] == file.uri_file
                    # TODO: https://github.com/vibhatha/pyiceberg_substrait/issues/3
            elif self._table_name:
                named_table = read_rel.named_table
                assert named_table.names[0] == self._table_name

        def visit_set(self, rel):
            pass

        def visit_sort(self, rel):
            pass

@pytest.fixture(scope="class", autouse=True)
def setup_and_teardown(request):
    print("Setup: initializing resources")
    request.cls.test_dir = tempfile.mkdtemp()

    yield

    print("Teardown: releasing resources")
    shutil.rmtree(request.cls.test_dir)

class TestDuckdbSubstrait:
    
    @pytest.fixture(autouse=True)
    def setup(self):
        self.con = duckdb.connect()
        self.con.install_extension("substrait")
        self.con.load_extension("substrait")
        
        data = [pa.array([1, 2, 3]), pa.array(["A", "B", "C"])]
        names = ["id", "name"]
        self.table = pa.Table.from_arrays(data, names=names)
        self.data_path = path.join(self.test_dir, "data.parquet")
        pq.write_table(self.table, self.data_path)

    
    def test_simple_plan(self):
        self.con.execute(query='CREATE TABLE SampleTable (id int,name text);')
        proto_bytes = self.con.get_substrait("SELECT * FROM SampleTable;").fetchone()[0]
        editor = SubstraitPlanEditor(proto_bytes)

        files = [self.data_path]
        file_formats = ["parquet"]
        update_visitor = RelUpdateVisitor(files=files, formats=file_formats)
        visit_and_update(editor.rel, update_visitor)
        
        proto_bytes = editor.plan.SerializeToString()
        
        query_result = self.con.from_substrait(proto=proto_bytes)
        result_table = query_result.to_arrow_table()
        assert result_table == self.table
       
    @pytest.mark.skip(reason="This test case only passes if Iceberg is configured with REST catalog and MinIO blobstore.") 
    def test_duckdb_wrapper(self):
        """
        NOTE: This test case only passes if Iceberg is configured with REST catalog
        and MinIO blobstore. 
        """
        self.con.execute(query='CREATE TABLE SampleTable (id int,name text);')
        sql_query = "SELECT * FROM SampleTable;"
        proto_bytes = self.con.get_substrait(sql_query).fetchone()[0]
        duckdb_substrait = DuckdbSubstrait(proto_bytes, "default", "/home/iceberg/notebooks/s3", "", sql_query)
        duckdb_substrait.execute()

    def test_duckdb_schema(self):
        create_schema = "CREATE SCHEMA myschema;"
        self.con.execute(create_schema)
        self.con.execute(query='CREATE TABLE myschema.SampleTable (id int,name text);')
        proto_bytes = self.con.get_substrait("SELECT * FROM myschema.SampleTable;").fetchone()[0]
        editor = SubstraitPlanEditor(proto_bytes)
        full_table_name = "myschema.SampleTable"
        named_table_update_visitor = NamedTableUpdateVisitor(full_table_name)
        visit_and_update(editor.rel, named_table_update_visitor)

        table_name_validate_visitor = RelValidateVisitor(files=None, formats=None, table_name=full_table_name)
        visit_and_update(editor.rel, table_name_validate_visitor)

        files = ["s3://mydata.parquet"]
        formats = ["parquet"]
        files_update_visitor = RelUpdateVisitor(files=files, formats=formats)
        visit_and_update(editor.rel, files_update_visitor)
        files_update_validate_visitor = RelValidateVisitor(files=files, formats=formats, table_name=None)
        visit_and_update(editor.rel, files_update_validate_visitor)

    def test_substrait_base_schema(self):
        create_schema = "CREATE SCHEMA myschema;"
        self.con.execute(create_schema)
        self.con.execute(query='CREATE TABLE myschema.SampleTable (id int,name text);')
        self.con.execute(query="INSERT INTO myschema.SampleTable (id, name) VALUES (1, 'A'), (2, 'B'), (3, 'C');")
        select_query = "SELECT * FROM myschema.SampleTable LIMIT 1;"
        proto_bytes = self.con.get_substrait(select_query).fetchone()[0]
        editor = SubstraitPlanEditor(proto_bytes)
        print(editor.plan)
        res = self.con.execute(select_query)
        res_ar_tb = res.fetch_arrow_table()
        print(res_ar_tb.to_pandas())
        
    def test_substrait_from_arrow(self):
        # connect to an in-memory database
        my_arrow = pa.Table.from_pydict({'a':[42]})

        # create the table "my_table" from the DataFrame "my_arrow"
        self.con.execute("CREATE TABLE my_table AS SELECT * FROM my_arrow").arrow()
        
        # insert into the table "my_table" from the DataFrame "my_arrow"
        self.con.execute("INSERT INTO my_table SELECT * FROM my_arrow").arrow()
        
        select_query = "SELECT * FROM my_table LIMIT 1;"
        proto_bytes = self.con.get_substrait(select_query).fetchone()[0]
        editor = SubstraitPlanEditor(proto_bytes)
        print(editor.plan)
        
        
    def test_schema_updator(self):
        from icetrait.substrait.visitor import SchemaUpdateVisitor
        
        my_arrow = pa.Table.from_pydict({'a':[42, 20, 21], 'b': ["a", "b", "c"]})
        editor = arrow_table_to_substrait(my_arrow)
        visitor = SchemaUpdateVisitor()
        visit_and_update(editor.rel, visitor)
        base_schema = visitor.base_schema
        
        assert base_schema.names == ['a', 'b']
        struct = base_schema.struct
        types = struct.types
        assert types[0].HasField("i64")
        assert types[1].HasField("varchar")
        assert types[0].i64.nullability == 1
        assert types[1].varchar.nullability == 1
        
    def test_schema_updator_empty_table(self):
        from icetrait.substrait.visitor import SchemaUpdateVisitor
        a = pa.array([], type=pa.int64())
        b = pa.array([], type=pa.utf8())
        my_arrow = pa.Table.from_arrays([a, b], names=["a", "b"])
        editor = arrow_table_to_substrait(my_arrow)
        visitor = SchemaUpdateVisitor()
        visit_and_update(editor.rel, visitor)
        base_schema = visitor.base_schema
        
        assert base_schema.names == ['a', 'b']
        struct = base_schema.struct
        types = struct.types
        assert types[0].HasField("i64")
        assert types[1].HasField("varchar")
        assert types[0].i64.nullability == 1
        assert types[1].varchar.nullability == 1
            
        
    def test_rel_names_update(self):
        from icetrait.substrait.visitor import SchemaUpdateVisitor
        
        my_arrow = pa.Table.from_pydict({'a':[42, 20, 21], 'b': ["a", "b", "c"]})
        editor = arrow_table_to_substrait(my_arrow)
        visitor = SchemaUpdateVisitor()
        visit_and_update(editor.rel, visitor)
        new_names = ['A', 'B']
        
        if editor.plan.relations:
            relations = editor.plan.relations
            if relations:
                if relations[0].HasField("root"):
                    rel_root = relations[0].root
                    for id, name in enumerate(new_names):
                        rel_root.names[id] = name

        assert editor.plan.relations[0].root.names == new_names
        
    
    def test_rel_names_update_diff_length(self):
        from icetrait.substrait.visitor import SchemaUpdateVisitor
        
        my_arrow = pa.Table.from_pydict({'a':[42, 20, 21], 'b': ["a", "b", "c"]})
        editor = arrow_table_to_substrait(my_arrow)
        visitor = SchemaUpdateVisitor()
        visit_and_update(editor.rel, visitor)
        new_names = ['A', 'B']
        
        if editor.plan.relations:
            relations = editor.plan.relations
            if relations:
                if relations[0].HasField("root"):
                    rel_root = relations[0].root
                    rel_root.names[:] = new_names
                    
        assert editor.plan.relations[0].root.names == new_names
        
    
    def test_base_schema_updator_end_to_end(self):
        from icetrait.substrait.visitor import SchemaUpdateVisitor
        
        my_arrow = pa.Table.from_pydict({'a':[42, 20, 21], 'b': ["a", "b", "c"]})
        editor = arrow_table_to_substrait(my_arrow)
        visitor = SchemaUpdateVisitor()
        visit_and_update(editor.rel, visitor)
        
        base_schema = visitor.base_schema
        
        files = ["s3://file.parquet"]
        formats = ["parquet"]
        update_visitor = RelUpdateVisitor(files=files, formats=formats, base_schema=base_schema)
        editor = arrow_table_to_substrait(my_arrow)
        visit_and_update(editor.rel, update_visitor)
        
        class ReadRelProjectVisitor(RelVisitor):
            def __init__(self, output_schema: List[str]):
                self._output_schema = output_schema
                
            @property
            def output_schema(self):
                return self._output_schema

            def visit_aggregate(self, rel):
                pass

            def visit_cross(self, rel):
                pass

            def visit_fetch(self, rel):
                pass

            def visit_filter(self, rel):
                pass

            def visit_join(self, rel):
                pass

            def visit_hashjoin(self, rel):
                pass

            def visit_merge(self, rel):
                pass

            def visit_project(self, project_rel):
                from substrait.gen.proto.algebra_pb2 import Expression
                if project_rel.expressions:
                    expressions = project_rel.expressions
                    len_out_schm = len(self.output_schema)
                    len_exprs = len(expressions)
                    if len_exprs < len_out_schm:
                        start_index = len_exprs
                        for _ in range(len_out_schm - len_exprs):
                            expression = Expression()
                            field_reference = expression.FieldReference()
                            root_reference = Expression.FieldReference.RootReference()
                            field_reference.direct_reference.struct_field.field = start_index
                            field_reference.root_reference.CopyFrom(root_reference)
                            expression.selection.CopyFrom(field_reference)
                            project_rel.expressions.append(expression)

            def visit_read(self, read_rel):
                from substrait.gen.proto.algebra_pb2 import Expression
                projection = read_rel.projection
                struct_items = projection.select.struct_items
                len_out_schm = len(self.output_schema)
                len_struct_items = len(struct_items)
                if len_struct_items < len_out_schm:
                    starting_index = len_struct_items
                    for _ in range(len_out_schm - len_struct_items):
                        struct_item = Expression.MaskExpression.StructItem()
                        struct_item.field = starting_index
                        starting_index = starting_index + 1
                        projection.select.struct_items.append(struct_item)

            def visit_set(self, rel):
                pass

            def visit_sort(self, rel):
                pass

        
        visit_and_update(editor.rel, ReadRelProjectVisitor(['A', 'B', 'C']))
        
        class ReadRelProjectValidateVisitor(RelVisitor):
            def __init__(self, expected_items:int=-1):
                self._expected_items = expected_items

            def visit_aggregate(self, rel):
                pass

            def visit_cross(self, rel):
                pass

            def visit_fetch(self, rel):
                pass

            def visit_filter(self, rel):
                pass

            def visit_join(self, rel):
                pass

            def visit_hashjoin(self, rel):
                pass

            def visit_merge(self, rel):
                pass

            def visit_project(self, project_rel):
                if project_rel.expressions:
                    expressions = project_rel.expressions
                    assert len(expressions) == self._expected_items

            def visit_read(self, read_rel):
                if read_rel.HasField("projection"):
                    if read_rel.projection.HasField("select"):
                        if read_rel.projection.select.struct_items:
                            projection = read_rel.projection
                            struct_items = projection.select.struct_items
                            assert len(struct_items) == self._expected_items
                
            def visit_set(self, rel):
                pass

            def visit_sort(self, rel):
                pass

        visit_and_update(editor.rel, ReadRelProjectValidateVisitor(3))

    def test_duckdb_additional_column_query(self):
        self.con.execute(query='CREATE TABLE SampleTable (id int,name text, age int);')
        proto_bytes = self.con.get_substrait("SELECT id, name, age FROM SampleTable;").fetchone()[0]
        editor = SubstraitPlanEditor(proto_bytes)

        files = [self.data_path]
        file_formats = ["parquet"]
        update_visitor = RelUpdateVisitor(files=files, formats=file_formats)
        visit_and_update(editor.rel, update_visitor)
        
        proto_bytes = editor.plan.SerializeToString()
        exp_err_msg = r"Binder Error: Positional reference 3 out of range \(total 2 columns\)"
        with pytest.raises(duckdb.BinderException, match=exp_err_msg):   
            self.con.from_substrait(proto=proto_bytes)
            
        