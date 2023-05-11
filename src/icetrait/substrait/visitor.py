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

class RelUpdateVisitor(RelVisitor):
    
    def __init__(self, files: List[str], formats: List[str]):
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
    
    def visit_hashjoin(self, rel: HashJoinRel):
        pass
    
    def visit_join(self, rel: JoinRel):
        pass
    
    def visit_merge(self, rel: MergeJoinRel):
        pass
    
    def visit_project(self, rel: ProjectRel):
        pass
    
    def visit_read(self, read_rel: ReadRel):
        print("visit read: ", self._formats)
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
        raise Exception("Invalid relation!")


@visit_and_update.register(ReadRel)
def _(rel: ReadRel, visitor: RelUpdateVisitor) -> RelType:
    visitor.visit_read(rel)

        
@visit_and_update.register(ProjectRel)
def _(rel: ProjectRel, visitor: RelUpdateVisitor) -> RelType:
    visitor.visit_project(rel)
    if rel.HasField("input"):
        visit_and_update(rel.input, visitor)
