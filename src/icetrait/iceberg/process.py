import os
from typing import List
from substrait.gen.proto.plan_pb2 import Plan
from substrait.gen.proto.algebra_pb2 import ReadRel

from pyiceberg.catalog import load_catalog
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table as IcebergTable

from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema, prune_columns
from pyiceberg.types import MapType, ListType

import pyarrow.dataset as ds
import pyarrow.parquet as pq

from icetrait.utils.files import get_filename_and_extension

import duckdb
import pyarrow as pa

from icetrait.substrait.visitor import SubstraitPlanEditor, SchemaUpdateVisitor, visit_and_update

import icetrait as icet

import warnings

ONE_MEGABYTE = 1024 * 1024
ICEBERG_SCHEMA = b"iceberg.schema"

## Introducing logging
import os
import logging
from icetrait.logging.logger import IcetraitLogger

icetrait_logger = IcetraitLogger(file_name="icetrait.iceberg.process.log")
logging.basicConfig(filename=icetrait_logger.log_path, encoding='utf-8', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
icetrait_logger.disabled = True if os.getenv("ICETRAIT_LOGGING") == "ENABLE" else False

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
                    logging.info("RelRoot : ", type(rel_root))
                    rel = rel_root.input
                    return self.visit_rel(rel)
        return None

   
    def visit_rel(self, rel):
        if rel:
            if rel.HasField("aggregate"):
                aggregate = rel.aggregate
                if aggregate.HasField("input"):
                    input = aggregate.input
                    logging.info(type(aggregate), type(input))
                    return self.visit_rel(input)
            elif rel.HasField("cross"):
                cross = rel.cross
                if cross.HasField("input"):
                    input = cross.input
                    logging.info(type(cross), type(input))
                    return self.visit_rel(input)
            elif rel.HasField("fetch"):
                fetch = rel.fetch
                if fetch.HasField("input"):
                    input = fetch.input
                    logging.info(type(fetch), type(input))
                    return self.visit_rel(input)
            elif rel.HasField("filter"):
                filter = rel.filter
                if filter.HasField("input"):
                    input = filter.input
                    logging.info(type(filter), type(input))
                    return self.visit_rel(input)
            elif rel.HasField("hash_join"):
                hash_join = rel.hash_join
                if hash_join.HasField("input"):
                    input = hash_join.input
                    logging.info(type(hash_join), type(input))
                    return self.visit_rel(input)
            elif rel.HasField("join"):
                join = rel.join
                if join.HasField("input"):
                    input = join.input
                    logging.info(type(join), type(input))
                    return self.visit_rel(input)
            elif rel.HasField("merge_join"):
                merge_join = rel.merge_join
                if merge_join.HasField("input"):
                    input = merge_join.input
                    logging.info(type(merge_join), type(input))
                    return self.visit_rel(input)
            if rel.HasField("project"):
                project = rel.project
                if project.HasField("input"):
                    input = project.input
                    logging.info(type(project), type(input))
                    return self.visit_rel(input)
            elif rel.HasField("read"):
                return rel.read
            elif rel.HasField("set"):
                set = rel.set
                if set.HasField("input"):
                    input = set.input
                    logging.info(type(set), type(input))
                    return self.visit_rel(input)
            elif rel.HasField("sort"):
                sort = rel.sort
                if sort.HasField("input"):
                    input = sort.input
                    logging.info(type(sort), type(input))
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
    
    def _find_field(self, file_project_schema, field_id):
        name = None
        try:
            name = file_project_schema.find_field(field_id).name
        except ValueError:
            return name
        return name
    
    def download(self, selected_fields:List[str]):
        sc = self.table.scan(selected_fields=selected_fields)
        table = sc.table
        tasks = sc.plan_files()
        scheme, _ = PyArrowFileIO.parse_location(table.location())
        current_table_schema = table.schema()
        projected_schema = sc.projection() # do we need to use this
        
        if isinstance(table.io, PyArrowFileIO):
            fs = table.io.get_fs(scheme)
        download_paths = []
        extensions = []
        # TODO: see if we can extract the file_schema, output_schema, output_field_names
        # physical_schema from this method itself, then we can use those values in RelUpdateVisitor
        # and update the file paths and base_schema from a single call.
        
        root_rel_names = []
        file_schema = None
        physical_schema = None
        base_schema = None
        projected_field_ids = None
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

                schema_raw = None
                if metadata := physical_schema.metadata:
                    schema_raw = metadata.get(ICEBERG_SCHEMA)
                if schema_raw is None:
                    raise ValueError(
                        "Iceberg schema is not embedded into the Parquet file, see https://github.com/apache/iceberg/issues/6505"
                    )
                file_schema = Schema.parse_raw(schema_raw)
                # note that the find_type(id) would retrieve the field based on the unique id
                # schema evolution is guaranteed by the unique id definition for each column 
                # irrespective of the RUD operation (READ, UPDATE, DELETE)
                projected_field_ids = {id for id in projected_schema.field_ids \
                                       if not isinstance(projected_schema.find_type(id), (MapType, ListType))}
                
                ## TODO: should we use the file_project_schema: https://github.com/vibhatha/pyiceberg_substrait/issues/27
                ## file_project_schema = prune_columns(file_schema, projected_field_ids, select_full_types=False)

                ## we use physical_schema of the file as the executable Substrait plan's base_schema

                ## for each file the names should be the same so just extract values for the first file
                if len(root_rel_names) == 0:
                    ## TODO: Evaluate this logic: https://github.com/vibhatha/pyiceberg_substrait/issues/28
                    ## This logic becomes faulty when the user ask for partial amount of columns
                    ## Older logic
                    ## for field in projected_schema.fields:
                    ##     root_rel_names.append(field.name)
                    for field in current_table_schema.fields:
                        root_rel_names.append(field.name)
                    
                        
                # get base_schema
                if base_schema is None:
                    ## NOTE: IMPORTANT
                    ## Since there is no direct method to convert a PyArrow Schema
                    ## into a Substrait schema, we would use a simple hack by creating
                    ## an empty table with the physical schema of the file with no data.
                    ## Then we use this table to generate a Substrait plan and from that
                    ## plan we extract the base_schema.
                    empty_table = pa.Table.from_pylist([], physical_schema)
                    logging.info("Table before update")
                    logging.info(empty_table)

                    # TODO: https://github.com/vibhatha/pyiceberg_substrait/issues/29
                    def get_absolute_name(field, reference_table):
                        if field not in reference_table.column_names:
                            # This scenario occurs when we have a rename-based schema evolution
                            # The file would have a different name compared to what is in the evolved
                            # schema. So we would find the field from the Iceberg table's schema.
                            # Then use that field_id to obtain the relative location in the
                            # reference table. This reference table contains the base_schema.
                            # TODO:
                            logging.info(f"{field} not in {reference_table.column_names}")
                            ref_field = current_table_schema.find_field(field)
                            field = reference_table.column_names[ref_field.field_id - 1] # -1 to get the index
                            logging.info("Rerouted: >> %s :: %s", ref_field, field)
                            return field
                        else:
                            return field

                    def create_columns_for_select(selected_fields:List[str], reference_table:pa.Table):
                        num_fields = len(selected_fields)
                        if len(selected_fields) == 1:
                            # choose single field vs * (all fields)
                            if selected_fields[0] == "*":
                                return selected_fields[0]
                            else:
                                return get_absolute_name(selected_fields[0], reference_table)
                        statement = ""
                        for idx, field in enumerate(selected_fields):
                            # if the field is not in the empty table
                            # we must track the correct name from table_schema
                            field = get_absolute_name(field, reference_table)
                            if idx != num_fields - 1:
                                statement = statement + field + ", "
                            else:
                                statement = statement + field
                        return statement

                    selected_columns = create_columns_for_select(selected_fields=selected_fields, reference_table=empty_table)
                    # generate a Substrait plan from table representing physical schema and only use selected columns
                    editor = arrow_table_to_substrait_with_select(empty_table, selected_columns=selected_columns)
                    schema_visitor = SchemaUpdateVisitor()
                    visit_and_update(editor.rel, schema_visitor)
                    base_schema = schema_visitor.base_schema
                    logging.info("*" * 80)
                    logging.info("Projected Schema")
                    logging.info(projected_schema)
                    logging.info("*" * 80)
                    logging.info("Projected Ids")
                    logging.info(projected_field_ids)
                    logging.info("+" * 80)
                    logging.info("Empty Table Based Plan")
                    logging.info("#" * 80)
                    logging.info(editor.plan)
                    logging.info("#" * 80)
                    logging.info("+" * 80)

        return download_paths, extensions, base_schema, root_rel_names, current_table_schema


def arrow_table_to_substrait(pyarrow_table: pa.Table):
    warnings.warn("This method is deprecated, use arrow_table_to_substrait_with_select. Will be dropped in stable release v0.0.1", DeprecationWarning)
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

def arrow_table_to_substrait_with_select(pyarrow_table: pa.Table, selected_columns:str):
    """
    **WARNING**: If parameter name of the first parameter is changed, that must be also changed
    where it's name is used as a string in the SQL queries used within the function
    definition.

    Args:
        pyarrow_table (pa.Table): input_table
        selected_columns (str): selected columns

    Returns:
        SubstraitPlanEditor : An interface which holds a SubstraitPlan in protobuf format
    """
    ## initialize duckdb
    con = duckdb.connect()
    con.install_extension("substrait")
    con.load_extension("substrait")
    ## NOTE: that the function argument pyarrow_table is used in the query
    # if the parameter name changed, please change the arrow_table_name
    arrow_table_name = "pyarrow_table"
    temp_table_name = "tmp_table"
    con.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
    con.execute(f"CREATE TABLE {temp_table_name} AS SELECT * FROM {arrow_table_name}").arrow()
    con.execute(f"INSERT INTO {temp_table_name} SELECT * FROM {arrow_table_name}").arrow()
    select_query = f"SELECT {selected_columns} FROM {temp_table_name};"
    proto_bytes = con.get_substrait(select_query).fetchone()[0]
    editor = SubstraitPlanEditor(proto_bytes)
    return editor
