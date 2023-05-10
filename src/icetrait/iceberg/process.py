from typing import List
from substrait.gen.proto.plan_pb2 import Plan
from substrait.gen.proto.algebra_pb2 import ReadRel

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