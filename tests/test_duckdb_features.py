import shutil, tempfile
from os import path

import duckdb
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from icetrait.substrait.visitor import RelVisitor, NamedTableUpdateVisitor, SubstraitPlanEditor, visit_and_update, RelUpdateVisitor
from icetrait.duckdb.wrapper import DuckdbSubstrait

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
        
    def test_duckdb_wrapper(self):
        """
        NOTE: This test case only passes if Iceberg is configured with REST catalog
        and MinIO blobstore. 
        """
        self.con.execute(query='CREATE TABLE SampleTable (id int,name text);')
        proto_bytes = self.con.get_substrait("SELECT * FROM SampleTable;").fetchone()[0]
        duckdb_substrait = DuckdbSubstrait(proto_bytes, "default", "/home/iceberg/notebooks/s3", "")
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
        files_update_visitor_validate_visitor = RelValidateVisitor(files=files, formats=formats, table_name=None)
        visit_and_update(editor.rel, files_update_visitor_validate_visitor)
