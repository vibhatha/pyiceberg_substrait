import os
from typing import List
from substrait.gen.proto.plan_pb2 import Plan
from substrait.gen.proto.algebra_pb2 import ReadRel

from pyiceberg.catalog import load_catalog
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table as IcebergTable

from pyiceberg.io.pyarrow import PyArrowFileIO

import pyarrow.dataset as ds
import pyarrow.parquet as pq

from icetrait.utils.files import get_filename_and_extension

import duckdb
import pyarrow as pa

from icetrait.substrait.visitor import SubstraitPlanEditor



ONE_MEGABYTE = 1024 * 1024

class ProcessSubstrait:
    
    def __init__(self, plan:str):
        self._plan = plan
        self._substrait_plan = Plan()
        self._substrait_plan.ParseFromString(self._plan)
    
    @property
    def plan(self):
        return self._substrait_plan
    
    def get_read_rel(self):
        if self._substrait_plan.relations:
            relations = self.plan.relations
            if relations:
                if relations[0].HasField("root"):
                    rel_root = relations[0].root
                    rel = rel_root.input
                    return self._get_read_rel(rel)
        return ReadRel()

    def _get_read_rel(self, rel):
        # TODO: test this function for a plan with a join/s
        if rel:
            if rel.HasField("aggregate"):
                aggregate = rel.aggregate
                if aggregate.HasField("input"):
                    input = aggregate.input
                    return self._get_read_rel(input)
            elif rel.HasField("cross"):
                cross = rel.cross
                if cross.HasField("input"):
                    input = cross.input
                    return self._get_read_rel(input)
            elif rel.HasField("fetch"):
                fetch = rel.fetch
                if fetch.HasField("input"):
                    input = fetch.input
                    return self._get_read_rel(input)
            elif rel.HasField("filter"):
                filter = rel.filter
                if filter.HasField("input"):
                    input = filter.input
                    return self._get_read_rel(input)
            elif rel.HasField("hash_join"):
                hash_join = rel.hash_join
                if hash_join.HasField("input"):
                    input = hash_join.input
                    return self._get_read_rel(input)
            elif rel.HasField("join"):
                join = rel.join
                if join.HasField("input"):
                    input = join.input
                    return self._get_read_rel(input)
            elif rel.HasField("merge_join"):
                merge_join = rel.merge_join
                if merge_join.HasField("input"):
                    input = merge_join.input
                    return self._get_read_rel(input)
            if rel.HasField("project"):
                project = rel.project
                if project.HasField("input"):
                    input = project.input
                    return self._get_read_rel(input)
            elif rel.HasField("read"):
                return rel.read
            elif rel.HasField("set"):
                set = rel.set
                if set.HasField("input"):
                    input = set.input
                    return self._get_read_rel(input)
            elif rel.HasField("sort"):
                sort = rel.sort
                if sort.HasField("input"):
                    input = sort.input
                    return self._get_read_rel(input)
            else:
                raise Exception("Invalid relation!")
        return None
        
    def update_local_files(self, files: List[str], file_formats: List[str]):
        read_rel = self.get_read_rel()
        local_files = read_rel.LocalFiles()
        for file, file_format in zip(files, file_formats):
            file_or_files = local_files.FileOrFiles()
            file_or_files.uri_file = file
            if file_format == "orc":
                orc = ReadRel.LocalFiles.FileOrFiles.OrcReadOptions()
                file_or_files.orc.CopyFrom(orc)
            elif file_format == "dwrf":
                dwrf = ReadRel.LocalFiles.FileOrFiles.DwrfReadOptions()
                file_or_files.dwrf.CopyFrom(dwrf)
            elif file_format == "parquet":
                parquet = ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions()
                file_or_files.parquet.CopyFrom(parquet)
            elif file_format == "arrow":
                arrow = ReadRel.LocalFiles.FileOrFiles.ArrowReadOptions()
                file_or_files.arrow.CopyFrom(arrow)
            else:
                raise ValueError(f"Unsupported file format {file_format}")
            local_files.items.append(file_or_files)
        read_rel.local_files.CopyFrom(local_files)

    
    def visitor_help(self):
        if self._substrait_plan.relations:
            relations = self.plan.relations
            if relations:
                if relations[0].HasField("root"):
                    rel_root = relations[0].root
                    print("RelRoot : ", type(rel_root))
                    rel = rel_root.input
                    return self.visit_rel(rel)
        return None

   
    def visit_rel(self, rel):
        if rel:
            if rel.HasField("aggregate"):
                aggregate = rel.aggregate
                if aggregate.HasField("input"):
                    input = aggregate.input
                    print(type(aggregate), type(input))
                    return self.visit_rel(input)
            elif rel.HasField("cross"):
                cross = rel.cross
                if cross.HasField("input"):
                    input = cross.input
                    print(type(cross), type(input))
                    return self.visit_rel(input)
            elif rel.HasField("fetch"):
                fetch = rel.fetch
                if fetch.HasField("input"):
                    input = fetch.input
                    print(type(fetch), type(input))
                    return self.visit_rel(input)
            elif rel.HasField("filter"):
                filter = rel.filter
                if filter.HasField("input"):
                    input = filter.input
                    print(type(filter), type(input))
                    return self.visit_rel(input)
            elif rel.HasField("hash_join"):
                hash_join = rel.hash_join
                if hash_join.HasField("input"):
                    input = hash_join.input
                    print(type(hash_join), type(input))
                    return self.visit_rel(input)
            elif rel.HasField("join"):
                join = rel.join
                if join.HasField("input"):
                    input = join.input
                    print(type(join), type(input))
                    return self.visit_rel(input)
            elif rel.HasField("merge_join"):
                merge_join = rel.merge_join
                if merge_join.HasField("input"):
                    input = merge_join.input
                    print(type(merge_join), type(input))
                    return self.visit_rel(input)
            if rel.HasField("project"):
                project = rel.project
                if project.HasField("input"):
                    input = project.input
                    print(type(project), type(input))
                    return self.visit_rel(input)
            elif rel.HasField("read"):
                return rel.read
            elif rel.HasField("set"):
                set = rel.set
                if set.HasField("input"):
                    input = set.input
                    print(type(set), type(input))
                    return self.visit_rel(input)
            elif rel.HasField("sort"):
                sort = rel.sort
                if sort.HasField("input"):
                    input = sort.input
                    print(type(sort), type(input))
                    return self.visit_rel(input)
            else:
                raise Exception("Invalid relation!")
        return None

class IcebergFileDownloader:
    """
    Note that this can be tested in an environment which supports
    Iceberg Catalogs.
    """
    def __init__(self, catalog:str, table:str, local_path:str):
        self._catalog = load_catalog(catalog)
        self._table = self._catalog.load_table(table)
        self._local_path = local_path
        
    @property
    def catalog(self) -> Catalog:
        return self._catalog
    
    @property
    def table(self) -> IcebergTable:
        return self._table
    
    def download(self):
        sc = self.table.scan()
        table = sc.table
        tasks = sc.plan_files()
        scheme, _ = PyArrowFileIO.parse_location(table.location())

        if isinstance(table.io, PyArrowFileIO):
            fs = table.io.get_fs(scheme)
        download_paths = []
        extensions = []
        for task in tasks:    
            _, parquet_file_path = PyArrowFileIO.parse_location(task.file.file_path)
            arrow_format = ds.ParquetFileFormat(pre_buffer=True, buffer_size=(ONE_MEGABYTE * 8))
            with fs.open_input_file(parquet_file_path) as fin:
                fragment = arrow_format.make_fragment(fin)
                physical_schema = fragment.physical_schema
                pyarrow_filter = None
                fragment_scanner = ds.Scanner.from_fragment(
                    fragment=fragment,
                    schema=physical_schema,
                    filter=pyarrow_filter,
                )
                arrow_table = fragment_scanner.to_table()
                filename, file_ext = get_filename_and_extension(parquet_file_path)
                save_file_path = os.path.join(self._local_path, filename + file_ext)
                pq.write_table(arrow_table, save_file_path)
                download_paths.append(save_file_path)
                extensions.append(file_ext.split(".")[1])
        return download_paths, extensions
    
    
# class SchemaEvolutionUtil:
    
#     def __init__(self, catalog_name, table_name, database_schema) -> None:
#         self._catalog_name = catalog_name
#         self._table_name = table_name
#         self._database_schema = database_schema
#         self._iceberg_catalog = None
#         self._iceberg_table = None
        
#     def load_catalog(self):
#         self._iceberg_catalog = load_catalog(self._catalog_name)
#         return self._iceberg_catalog
        
#     def load_table(self):
#         self._iceberg_table = self._catalog.load_table(self._table_name)
#         return self._iceberg_table
        
#     def load_physical_file(self):
#         sc = self.table.scan()
#         table = sc.table
#         tasks = sc.plan_files()
#         scheme, _ = PyArrowFileIO.parse_location(table.location())
        
        
#         if isinstance(table.io, PyArrowFileIO):
#             fs = table.io.get_fs(scheme)
        
#         if fs is None:
#             raise ValueError(f"Couldn't load file system for the provide catalog {self._catalog_name}, table {self._table_name}")
        
#         for task in tasks:    
#             _, parquet_file_path = PyArrowFileIO.parse_location(task.file.file_path)
#             arrow_format = ds.ParquetFileFormat(pre_buffer=True, buffer_size=(ONE_MEGABYTE * 8))
#             with fs.open_input_file(parquet_file_path) as fin:
#                 fragment = arrow_format.make_fragment(fin)
#                 physical_schema = fragment.physical_schema
#                 print(physical_schema)

def arrow_table_to_substrait(pyarrow_table: pa.Table):
    ## initialize duckdb
    con = duckdb.connect()
    con.install_extension("substrait")
    con.load_extension("substrait")
    # Note that the function argument pyarrow_table is used in the query
    # if the parameter name changed, please change the arrow_table_name
    arrow_table_name = "pyarrow_table"
    temp_table_name = "tmp_table"
    con.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
    con.execute(f"CREATE TABLE {temp_table_name} AS SELECT * FROM {arrow_table_name}").arrow()
    con.execute(f"INSERT INTO {temp_table_name} SELECT * FROM {arrow_table_name}").arrow()
    select_query = f"SELECT * FROM {temp_table_name};"
    proto_bytes = con.get_substrait(select_query).fetchone()[0]
    editor = SubstraitPlanEditor(proto_bytes)
    return editor
