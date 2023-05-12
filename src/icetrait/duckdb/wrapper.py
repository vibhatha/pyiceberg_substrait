import duckdb

from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import Table as IcebergTable
import pyarrow.dataset as ds
import pyarrow.parquet as pq

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
    
    def __init__(self, plan: SubstraitPlan, catalog_name:str, local_path:str, duckdb_schema:str):
        self._plan = plan
        self._substrait_plan = SubstraitPlan()
        self._substrait_plan.ParseFromString(self._plan)
        self._catalog_name = catalog_name
        self._duckdb_schema = duckdb_schema
        self._files = None
        self._formats = None
        self._updated_plan = None
        self._local_path = local_path
        self._con = None
        self._initialize()
        self._get_table_name()
        
    @property
    def plan(self):
        return self._substrait_plan
    
    def _initialize(self):
        self._con = duckdb.connect()
        self._con.install_extension("substrait")
        self._con.load_extension("substrait")

        self._con.install_extension("httpfs")
        self._con.load_extension("httpfs")
        
        return self._con
    
    @property
    def table_name(self):
        return self._get_table_name()
    
    def _get_table_name(self):
        extract_visitor = ExtractTableVisitor()
        rel = extract_rel_from_plan(self._plan)
        visit_and_update(rel, extract_visitor)
        return extract_visitor.table_names[0]

    def update_schema(self):
        table_name = self.table_name
        full_table_name = f"{self._duckdb_schema}.{table_name}"
        editor = SubstraitPlanEditor(self._plan)
        named_table_update_visitor = NamedTableUpdateVisitor(full_table_name)
        visit_and_update(editor.rel, named_table_update_visitor)
        self._updated_plan = editor.plan

    def update_with_local_file_paths(self):
        # this method would update the passed Substrait plan
        # with s3 urls to local files 
        # Issue: https://github.com/duckdb/duckdb/discussions/7252
        table_name = self.table_name
        downloader = IcebergFileDownloader(catalog=self._catalog_name, table=table_name, local_path=self._local_path)
        self._files, self._formats = downloader.download()
        update_visitor = RelUpdateVisitor(files=self._files, formats=self._formats)
        editor = SubstraitPlanEditor(self._updated_plan.SerializeToString())
        visit_and_update(editor.rel, update_visitor)
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
    wrapper = DuckdbSubstrait(plan=plan, catalog_name=catalog_name, local_path=local_path, duckdb_schema=duckdb_schema)
    wrapper.update_schema()
    wrapper.update_with_local_file_paths()
    return wrapper.execute()
