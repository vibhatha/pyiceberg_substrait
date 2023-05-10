import ibis
from ibis_substrait.compiler.core import SubstraitCompiler
from google.protobuf import json_format
from google.protobuf.json_format import MessageToJson
from substrait.gen.proto.algebra_pb2 import AggregateRel, CrossRel, FetchRel, FilterRel, HashJoinRel, JoinRel, MergeJoinRel, ProjectRel, ReadRel, SetRel, SortRel

from icetrait.iceberg.process import ProcessSubstrait

def test_substrait_basic():
    tb = ibis.table([("id", "date"), ("bid", "float"), ("ask", "int32"), ("symbol", "int64")], "t",)
    query = tb.select(["id", "bid", "ask", "symbol"])
    compiler = SubstraitCompiler()
    protobuf_msg = compiler.compile(query).SerializeToString()

    psb = ProcessSubstrait(protobuf_msg)

    read_rel = psb.get_read_rel()
    files = ["s3://data/p1.parquet", "s3://data/p2.orc", "s3://data/p3.dwrf", "s3://data/p4.arrow"]
    file_formats = ["parquet", "orc", "dwrf", "orc"]
    psb.update_local_files(files, file_formats)

    read_rel = psb.get_read_rel()
    local_files = read_rel.local_files
    for id, file in enumerate(local_files.items):
        assert files[id] == file.uri_file
        # TODO: https://github.com/vibhatha/pyiceberg_substrait/issues/3
        

def test_visitor():
    tb = ibis.table([("id", "date"), ("bid", "float"), ("ask", "int32"), ("symbol", "int64")], "t",)
    query = tb.select(["id", "bid", "ask", "symbol"])
    compiler = SubstraitCompiler()
    protobuf_msg = compiler.compile(query).SerializeToString()

    from icetrait.substrait.visitor import RelVisitor, SubstraitPlanEditor, visit_and_update, RelUpdateVisitor
    
    class RelValidateVisitor(RelVisitor):
        
        def __init__(self, files, formats):
            self._files = files
            self._formats = formats
        
        def visit_aggregate(self, rel: AggregateRel):
            pass
        
        def visit_cross(self, rel: CrossRel):
            pass
        
        def visit_fetch(self, rel: FetchRel):
            pass
        
        def visit_filter(self, rel: FilterRel):
            pass
        
        def visit_join(self, rel: JoinRel):
            pass
        
        def visit_hashjoin(self, rel: HashJoinRel):
            pass
        
        def visit_merge(self, rel: MergeJoinRel):
            pass
        
        def visit_project(self, rel: ProjectRel):
            pass
        
        def visit_read(self, read_rel: ReadRel):
            local_files = read_rel.local_files
            for id, file in enumerate(local_files.items):
                assert self._files[id] == file.uri_file
                # TODO: https://github.com/vibhatha/pyiceberg_substrait/issues/3
        
        def visit_set(self, rel: SetRel):
            pass
        
        def visit_sort(self, rel: SortRel):
            pass
    
    
    editor = SubstraitPlanEditor(protobuf_msg)
    print("-" * 80)
    print(editor.rel)
    files = ["s3://data/p1.parquet", "s3://data/p2.orc", "s3://data/p3.dwrf", "s3://data/p4.arrow"]
    file_formats = ["parquet", "orc", "dwrf", "orc"]
    update_visitor = RelUpdateVisitor(files=files, formats=file_formats)
    visit_and_update(editor.rel, update_visitor)
    
    print(editor.plan)
    
    validate_visitor = RelValidateVisitor(files=files, formats=file_formats)
    visit_and_update(editor.rel, validate_visitor)
    
    
    
        