import ibis
from ibis_substrait.compiler.core import SubstraitCompiler
from google.protobuf import json_format
from google.protobuf.json_format import MessageToJson

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
        # TODO: figure out a way to validate file.format update
    
        