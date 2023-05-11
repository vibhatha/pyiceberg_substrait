import shutil, tempfile
from os import path

import duckdb
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from icetrait.substrait.visitor import RelVisitor, SubstraitPlanEditor, visit_and_update, RelUpdateVisitor
from icetrait.duckdb.wrapper import DuckdbSubstrait

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
        duckdb_substrait = DuckdbSubstrait(proto_bytes, "default", "/home/iceberg/notebooks/s3")
        results = duckdb_substrait.execute()
        print(results)
