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


def test_metadata_extraction():
    tb = ibis.table([("id", "date"), ("bid", "float"), ("ask", "int32"), ("symbol", "int64")], "t",)
    query = tb.select(["id", "bid", "ask", "symbol"])
    compiler = SubstraitCompiler()
    protobuf_msg = compiler.compile(query).SerializeToString()

    from icetrait.substrait.visitor import ExtractTableVisitor, extract_rel_from_plan, visit_and_update
    
    extract_visitor = ExtractTableVisitor()
    rel = extract_rel_from_plan(protobuf_msg)
    visit_and_update(rel, extract_visitor)
    
    print(extract_visitor.table_names)

    assert extract_visitor.table_names == ['t']


def test_update_readrel_projection():
    import duckdb
    from substrait.gen.proto.plan_pb2 import Plan as SubstraitPlan
    con = duckdb.connect()
    con.install_extension("substrait")
    con.load_extension("substrait")
    con.execute("""
                CREATE TABLE simple_table (
        A bigint,
        B bigint,
        C bigint,
        D bigint,
        E bigint
    );
    """)
    
    sql_query = "SELECT A, B, C, E from simple_table;"
    plan = con.get_substrait(sql_query).fetchone()[0]
    substrait_plan = SubstraitPlan()
    substrait_plan.ParseFromString(plan)


    from icetrait.substrait.visitor import RelVisitor, SubstraitPlanEditor, visit_and_update
    
    class RelValidateVisitor(RelVisitor):
        
        def __init__(self, files, formats, project_ids=None, field_indices=None):
            self._files = files
            self._formats = formats
            self._project_ids = project_ids
            self.base_schema = None
            self._field_indices = field_indices
        
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
        
        def visit_project(self, project_rel: ProjectRel):
            from substrait.gen.proto.algebra_pb2 import Expression
            expressions = []
            for field_index in self._field_indices:
                expression = Expression()
                field_reference = expression.FieldReference()
                root_reference = Expression.FieldReference.RootReference()
                field_reference.direct_reference.struct_field.field = field_index
                field_reference.root_reference.CopyFrom(root_reference)
                expression.selection.CopyFrom(field_reference)
                expressions.append(expression)
            del project_rel.expressions[:]
            project_rel.expressions.extend(expressions)
        
        def visit_read(self, read_rel: ReadRel):
            self.base_schema = read_rel.base_schema
            if self._project_ids:
                if read_rel.HasField("projection"):
                    if read_rel.projection.HasField("select"):
                        if read_rel.projection.select.struct_items:
                            from substrait.gen.proto.algebra_pb2 import Expression
                            projection = read_rel.projection
                            new_struct_items = projection.select.struct_items
                            new_projection = Expression.MaskExpression()
                            for project_id in self._project_ids:
                                struct_item = Expression.MaskExpression.StructItem()
                                struct_item.field = project_id
                                new_projection.select.struct_items.append(struct_item)
                            projection.CopyFrom(new_projection)
        
        def visit_set(self, rel: SetRel):
            pass
        
        def visit_sort(self, rel: SortRel):
            pass
    
    
    editor = SubstraitPlanEditor(substrait_plan.SerializeToString())
    print("-" * 80)
    
    files = ["s3://data/p1.parquet", "s3://data/p2.orc", "s3://data/p3.dwrf", "s3://data/p4.arrow"]
    file_formats = ["parquet", "orc", "dwrf", "orc"]
    
    validate_visitor = RelValidateVisitor(files=files, formats=file_formats, project_ids=[5, 6, 7], field_indices=[0, 1, 2])
    visit_and_update(editor.rel, validate_visitor)
    
    base_schema = validate_visitor.base_schema
    names = base_schema.names


    print(len(names))
