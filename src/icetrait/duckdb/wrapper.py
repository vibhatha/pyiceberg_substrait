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
                                        RelUpdateVisitor,
                                        SubstraitPlanEditor,
                                        extract_rel_from_plan,
                                        visit_and_update
                                        )

class DuckdbSubstrait:
    
    def __init__(self, plan: SubstraitPlan, catalog_name:str, local_path:str):
        self._plan = plan
        self._substrait_plan = SubstraitPlan()
        self._substrait_plan.ParseFromString(self._plan)
        self._catalog_name = catalog_name
        self._files = None
        self._formats = None
        self._updated_plan = None
        self._local_path = local_path
        self._get_table_name()
        
    @property
    def plan(self):
        return self._substrait_plan
    
    def initialize(self):
        con = duckdb.connect()
        con.install_extension("substrait")
        con.load_extension("substrait")

        con.install_extension("httpfs")
        con.load_extension("httpfs")
        
        return con
    
    @property
    def table_name(self):
        return self._get_table_name(self)
    
    def _get_table_name(self):
        extract_visitor = ExtractTableVisitor()
        rel = extract_rel_from_plan(self._plan)
        visit_and_update(rel, extract_visitor)
        return extract_visitor.table_names[0]
    
    def _update_with_local_file_paths(self):
        # this method would update the passed Substrait plan
        # with s3 urls to local files 
        # Issue: https://github.com/duckdb/duckdb/discussions/7252
        downloader = IcebergFileDownloader(catalog='default', table=self.table_name, local_path=self._local_path)
        self._files, self._formats = downloader.download()
        editor = SubstraitPlanEditor(self._plan)
        update_visitor = RelUpdateVisitor(files=self._files, formats=self._formats)
        visit_and_update(editor.rel, update_visitor)
    
        self._updated_plan = editor.plan
    
    def execute(self):
        # run the updated Substrait plan with DuckDb
        pass

        