import ibis
from ibis_substrait.compiler.core import SubstraitCompiler
from google.protobuf import json_format

from icetrait.iceberg.process import ProcessSubstrait

from substrait.gen.proto.algebra_pb2 import ReadRel

def to_icetrait(rel):
    if rel:
        if rel.HasField("aggregate"):
            aggregate = rel.aggregate
            if aggregate.HasField("input"):
                input = aggregate.input
                return to_icetrait(input)
            else:
                return
        elif rel.HasField("cross"):
            cross = rel.cross
            if cross.HasField("input"):
                input = cross.input
                return to_icetrait(input)
            else:
                return
        elif rel.HasField("fetch"):
            print("Fetch")
            fetch = rel.fetch
            if fetch.HasField("input"):
                input = fetch.input
                return to_icetrait(input)
            else:
                return
        elif rel.HasField("filter"):
            print("Fetch")
            filter = rel.filter
            if filter.HasField("input"):
                input = filter.input
                return to_icetrait(input)
            else:
                return
        elif rel.HasField("hash_join"):
            print("HashJoin")
            hash_join = rel.hash_join
            if hash_join.HasField("input"):
                input = hash_join.input
                return to_icetrait(input)
            else:
                return
        elif rel.HasField("join"):
            print("HashJoin")
            join = rel.join
            if join.HasField("input"):
                input = join.input
                return to_icetrait(input)
            else:
                return
        elif rel.HasField("merge_join"):
            print("MergeJoin")
            merge_join = rel.merge_join
            if merge_join.HasField("input"):
                input = merge_join.input
                return to_icetrait(input)
            else:
                return
        if rel.HasField("project"):
            print("Project")
            project = rel.project
            if project.HasField("input"):
                input = project.input
                return to_icetrait(input)
            else:
                return
        elif rel.HasField("read"):
            print("Read")
            read = rel.read
            return read
        elif rel.HasField("set"):
            print("Set")
            set = rel.set
            if set.HasField("input"):
                input = set.input
                return to_icetrait(input)
            else:
                return
        elif rel.HasField("sort"):
            print("Sort")
            sort = rel.sort
            if sort.HasField("input"):
                input = sort.input
                return to_icetrait(input)
            else:
                return
        else:
            raise Exception("Invalid relation!")
    
    return

#def test_substrait_basic():
tb = ibis.table([("id", "date"), ("bid", "float"), ("ask", "int32"), ("symbol", "int64")], "t",)
query = tb.select(["id", "bid", "ask", "symbol"])
compiler = SubstraitCompiler()
protobuf_msg = compiler.compile(query).SerializeToString()
print(protobuf_msg)

psb = ProcessSubstrait(protobuf_msg)
print(psb.plan)

for attr in dir(psb.plan):
    print(attr)

relations = psb.plan.relations
print(type(relations))
print("-" * 100)
    
rel_root = relations[0].root
print(type(rel_root))
rel = rel_root.input
print(type(rel))
print("attrs(rel)")
print(dir(rel))

print(type(rel.project))
print(type(rel.project.input))
print()
project = rel.project

read = project.input
if read:
    print(read)
else:
    print("end")
from substrait.gen.proto.algebra_pb2 import ReadRel
read_rel = ReadRel()
print(dir(read_rel))


read_rel = to_icetrait(rel)
print(read_rel)

print(dir(read_rel))

if read_rel.HasField("named_table"):
    named_table = read_rel.named_table
    print(type(named_table), named_table, named_table.names)
    #named_table_proto = NamedTable()
    # proto = read_rel.NamedTable()
    print(dir(read_rel))
    if read_rel.HasField("local_files"):
        print(read_rel.local_files)
    else:
        print("local files setup")
        local_files = read_rel.LocalFiles()
        file_or_files = local_files.FileOrFiles()
        print("local_files")
        print(dir(local_files))
        print("file_or_files")
        print(dir(file_or_files))
        file_or_files.uri_file = "s3://my_file.parquet"
        # TODO updating files
        
