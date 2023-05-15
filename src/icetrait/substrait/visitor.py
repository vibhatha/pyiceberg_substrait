from abc import ABC, abstractmethod
from substrait.gen.proto.plan_pb2 import Plan as SubstraitPlan
from substrait.gen.proto.algebra_pb2 import (ReadRel,
    ProjectRel,
    AggregateRel,
    CrossRel,
    FetchRel,
    FilterRel,
    HashJoinRel,
    JoinRel,
    MergeJoinRel,
    SetRel,
    SortRel,
    RelRoot,
    Rel,
)
from typing import List, TypeVar


RelType = TypeVar("RelType")

from functools import singledispatch

class RelVisitor(ABC):
    
    @abstractmethod
    def visit_read(self, rel: ReadRel):
        pass
    
    @abstractmethod
    def visit_project(self, rel: ProjectRel):
        pass
    
    @abstractmethod
    def visit_aggregate(self, rel: AggregateRel):
        pass
    
    @abstractmethod
    def visit_cross(self, rel: CrossRel):
        pass
    
    @abstractmethod
    def visit_fetch(self, rel: FetchRel):
        pass
    
    @abstractmethod
    def visit_filter(self, rel: FilterRel):
        pass
    
    @abstractmethod
    def visit_hashjoin(self, rel: HashJoinRel):
        pass
    
    @abstractmethod
    def visit_join(self, rel: JoinRel):
        pass
    
    @abstractmethod
    def visit_merge(self, rel: MergeJoinRel):
        pass
    
    @abstractmethod
    def visit_set(self, rel: SetRel):
        pass
    
    @abstractmethod
    def visit_sort(self, rel: SortRel):
        pass

class IcebergSubstraitRelVisitor(RelVisitor):
    
    def visit_aggregate(self, rel: AggregateRel):
        pass
    
    def visit_cross(self, rel: CrossRel):
        pass
    
    def visit_fetch(self, rel: FetchRel):
        pass
    
    def visit_filter(self, rel: FilterRel):
        pass
    
    def visit_hashjoin(self, rel: HashJoinRel):
        pass
    
    def visit_join(self, rel: JoinRel):
        pass
    
    def visit_merge(self, rel: MergeJoinRel):
        pass
    
    def visit_project(self, rel: ProjectRel):
        pass
    
    @abstractmethod
    def visit_read(self, rel: ReadRel):
        """_summary_

        Args:
            rel (ReadRel): _description_
        """
    
    def visit_set(self, rel: SetRel):
        pass
    
    def visit_sort(self, rel: SortRel):
        pass

class RelUpdateVisitor(RelVisitor):
    ## TODO: rename this to ReadRelUpdateVisitor
    
    def __init__(self, files: List[str], formats: List[str], base_schema=None, output_names=None):
        self._files = files
        self._formats = formats
        self._base_schema = base_schema
        self._output_names = output_names
    
    def visit_aggregate(self, rel: AggregateRel):
        pass
    
    def visit_cross(self, rel: CrossRel):
        pass
    
    def visit_fetch(self, rel: FetchRel):
        pass
    
    def visit_filter(self, rel: FilterRel):
        pass
    
    def visit_hashjoin(self, rel: HashJoinRel):
        pass
    
    def visit_join(self, rel: JoinRel):
        pass
    
    def visit_merge(self, rel: MergeJoinRel):
        pass
    
    def visit_project(self, rel: ProjectRel):
        pass
    
    def visit_read(self, read_rel: ReadRel):
        # TODO: optimize this via a Visitor
        local_files = read_rel.LocalFiles()
        for file, file_format in zip(self._files, self._formats):
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
        
        if self._base_schema:
            read_rel.base_schema.CopyFrom(self._base_schema)
        
        # TODO: optimize this logic using a visitor
        if self._output_names:
            if read_rel.HasField("projection"):
                if read_rel.projection.HasField("select"):
                    if read_rel.projection.select.HasField("struct_items"):
                        from substrait.gen.proto.algebra_pb2 import Expression
                        struct_items = read_rel.projection.select.struct_items
                        len_out_schm = len(self._output_names)
                        len_struct_items = len(struct_items)
                        if len_struct_items < len_out_schm:
                            starting_index = len_struct_items
                            for i in range(len_out_schm - len_struct_items):
                                struct_item = Expression.MaskExpression.StructItem()
                                struct_item.field = starting_index
                                starting_index = starting_index + 1
                                read_rel.projection.select.struct_items.append(struct_item)
        

    def visit_set(self, rel: SetRel):
        pass
    
    def visit_sort(self, rel: SortRel):
        pass

class ExtractTableVisitor(RelVisitor):
    
    def __init__(self) -> None:
        self._table_names = None
        
    @property
    def table_names(self):
        return self._table_names
        
    def visit_aggregate(self, rel: AggregateRel):
        pass
    
    def visit_cross(self, rel: CrossRel):
        pass
    
    def visit_fetch(self, rel: FetchRel):
        pass
    
    def visit_filter(self, rel: FilterRel):
        pass
    
    def visit_hashjoin(self, rel: HashJoinRel):
        pass
    
    def visit_join(self, rel: JoinRel):
        pass
    
    def visit_merge(self, rel: MergeJoinRel):
        pass
    
    def visit_project(self, rel: ProjectRel):
        pass
    
    def visit_read(self, read_rel: ReadRel):
        named_table = read_rel.named_table
        self._table_names = named_table.names
        
    def visit_set(self, rel: SetRel):
        pass
    
    def visit_sort(self, rel: SortRel):
        pass
    

# TODO: Think of a better way to do the job done by
# SubstraitPlanEditor vs extract_rel_from_plan
# The class and function does the same

class SubstraitPlanEditor:
    
    def __init__(self, plan: SubstraitPlan):
        self._plan = plan
        self._substrait_plan = SubstraitPlan()
        self._substrait_plan.ParseFromString(self._plan)
        
    @property
    def plan(self):
        return self._substrait_plan
        
    @property
    def rel(self):
        if self._substrait_plan.relations:
            relations = self.plan.relations
            if relations:
                if relations[0].HasField("root"):
                    rel_root = relations[0].root
                    rel = rel_root.input
                    return rel
        return None
    
def extract_rel_from_plan(plan: SubstraitPlan):
    substrait_plan = SubstraitPlan()
    substrait_plan.ParseFromString(plan)
    if substrait_plan.relations:
            relations = substrait_plan.relations
            if relations:
                if relations[0].HasField("root"):
                    rel_root = relations[0].root
                    rel = rel_root.input
                    return rel
    return None
    
    
@singledispatch
def visit_and_update(rel, visitor: RelUpdateVisitor) -> RelType:
    raise ValueError(f"Unsupported relation: {type(rel)}")

@visit_and_update.register(Rel)
def _(rel: Rel, visitor: RelUpdateVisitor) -> RelType:
    if rel.HasField("aggregate"):
        visit_and_update(rel.aggregate, visitor)
    elif rel.HasField("cross"):
        visit_and_update(rel.cross, visitor)
    elif rel.HasField("fetch"):
        print(">>>>>>> rel.HasField('fetch')")
        visit_and_update(rel.fetch, visitor)
    elif rel.HasField("filter"):
        visit_and_update(rel.filter, visitor)
    elif rel.HasField("hash_join"):
        visit_and_update(rel.hash_join, visitor)
    elif rel.HasField("join"):
        visit_and_update(rel.join, visitor)
    elif rel.HasField("merge_join"):
        visit_and_update(rel.merge_join, visitor)
    if rel.HasField("project"):
        visit_and_update(rel.project, visitor)
    elif rel.HasField("read"):
        visit_and_update(rel.read, visitor)
    elif rel.HasField("set"):
        visit_and_update(rel.set, visitor)
    elif rel.HasField("sort"):
        visit_and_update(rel.sort, visitor)
    else:
        val = rel.HasField("fetch")
        raise Exception(f"Invalid relation! {val}")


@visit_and_update.register(ReadRel)
def _(rel: ReadRel, visitor: RelUpdateVisitor) -> RelType:
    visitor.visit_read(rel)

        
@visit_and_update.register(ProjectRel)
def _(rel: ProjectRel, visitor: RelUpdateVisitor) -> RelType:
    visitor.visit_project(rel)
    if rel.HasField("input"):
        visit_and_update(rel.input, visitor)


@visit_and_update.register(FetchRel)
def _(rel: FetchRel, visitor: RelUpdateVisitor):
    print("@visit_and_update.register(FetchRel)")
    visitor.visit_fetch(rel)
    if rel.HasField("input"):
        visit_and_update(rel.input, visitor)



## Helper Visitor to update the schema information in a DuckDB generated
## Substrait plan. Even when schema is included in a query, it doesn't generate
## namedTable with the `schema_name.table_name` format. 

class NamedTableUpdateVisitor(RelVisitor):
        def __init__(self, table_name):
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
            named_table = read_rel.NamedTable()
            named_table.names.append(self._table_name)
            read_rel.named_table.CopyFrom(named_table)

        def visit_set(self, rel):
            pass

        def visit_sort(self, rel):
            pass


class SchemaUpdateVisitor(RelVisitor):
        def __init__(self):
            self._base_schema = None
            
        @property
        def base_schema(self):
            return self._base_schema

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
            self._base_schema = read_rel.base_schema

        def visit_set(self, rel):
            pass

        def visit_sort(self, rel):
            pass