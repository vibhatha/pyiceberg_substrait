# TODO:

# 1. Create Two parquet files
#     The first file would have columns A, B, C, D
#     The second file would have columns A, B, C, D, E
    
# 2. Generate A Substrait plan such that we will have a union operation
#     so that we have two ReadRel and each would take one file. 
    
# 3. See if we can execute this in Substriat

# In SQL we cannot union two tables with different schemas, so we have to bring them 
# all to the same page.

#################
### Algorithm ###
#################

# 1. Find out the `base_schema` with the highest number of fields
# 2. Per table find out the non-common fields


import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import duckdb

import os
import shutil
import tempfile

from icetrait.iceberg.process import arrow_table_to_substrait_with_select
from icetrait.iceberg.process import SubstraitPlanEditor


import os
os.environ['ICETRAIT_LOG_DIR'] = '/home/asus/sandbox/icetrait_logs'
os.environ['ICETRAIT_LOGGING'] = 'DISABLE'
from icetrait.duckdb.wrapper import DuckdbSubstrait
from icetrait.substrait.visitor import RelVisitor, ExtractTableVisitor, visit_and_update, RelUpdateVisitor, SchemaUpdateVisitor

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
    Rel
)

from substrait.gen.proto.plan_pb2 import PlanRel

d1 = {
    "A": [1, 2, 3],
    "B": [2, 3, 4],
    "C": [3, 4, 5],
    "D": [4, 5, 6]
}

d2 = {
    "A": [11, 12, 13],
    "B": [12, 13, 14],
    "C": [13, 14, 15],
    "D": [14, 15, 16],
    "E": [15, 16, 17]
}

t1 = pa.Table.from_pydict(d1)
t2 = pa.Table.from_pydict(d2)

test_dir = "/home/asus/data/schema_evolution/advanced/"#tempfile.mkdtemp()
tmp_file_path_1 = os.path.join(test_dir, "d1.parquet")
tmp_file_path_2 = os.path.join(test_dir, "d2.parquet")

pq.write_table(t1, tmp_file_path_1)
pq.write_table(t2, tmp_file_path_2)

assert t1 == pq.read_table(tmp_file_path_1)
assert t2 == pq.read_table(tmp_file_path_2)


def setup_duckdb():
    con = duckdb.connect()
    create_schema = f"CREATE SCHEMA nyc_demo;"
    creation_query1 = f"""
    CREATE TABLE t1 (
        A bigint,
        B bigint,
        C bigint, 
        D bigint
    );
    """
    creation_query2 = f"""
    CREATE TABLE t2 (
        A bigint,
        B bigint,
        C bigint, 
        D bigint,
        E bigint
    );
    """
    con.execute(create_schema)
    con.execute(creation_query1)
    con.execute(creation_query2)
    return con

con = setup_duckdb()

con.install_extension("substrait")
con.load_extension("substrait")

# create a sample union

union_query = """
select * from t1
union
select * from t2
"""

## NOTE: duckdb.BinderException: Binder Error: Set operations can only apply to expressions with the same number of result columns
# union_plan = con.get_substrait(union_query).fetchone()[0]


editor_1 = arrow_table_to_substrait_with_select(t1, "*")
editor_2 = arrow_table_to_substrait_with_select(t2, "*")

schema_visitor = SchemaUpdateVisitor()
visit_and_update(editor_1.rel, schema_visitor)
base_schema_1 = schema_visitor.base_schema

schema_visitor = SchemaUpdateVisitor()
visit_and_update(editor_2.rel, schema_visitor)
base_schema_2 = schema_visitor.base_schema

update_visitor = RelUpdateVisitor(files=[tmp_file_path_1], formats=['parquet'], base_schema=base_schema_1)
visit_and_update(editor_1.rel, update_visitor)

update_visitor = RelUpdateVisitor(files=[tmp_file_path_2], formats=['parquet'], base_schema=base_schema_2)
visit_and_update(editor_2.rel, update_visitor)

extract_visitor_1 = ExtractTableVisitor()
visit_and_update(editor_1.rel, extract_visitor_1)

extract_visitor_2 = ExtractTableVisitor()
visit_and_update(editor_2.rel, extract_visitor_2)

class UnionPlanVisitor(RelVisitor):
    
    def __init__(self, num_missing_fields) -> None:
        self._num_missing_fields = num_missing_fields
        
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
    
    def visit_project(self, project_rel: ProjectRel):
        print("visit_project")
        from substrait.gen.proto.algebra_pb2 import Expression
        if project_rel.expressions and self._num_missing_fields > 0:
            expressions = project_rel.expressions
            for field_index in range(self._num_missing_fields):
                    expression = Expression()
                    #print(dir(expression))
                    literal = expression.Literal()
                    #print(dir(literal))
                    i64 = literal.i64
                    none_to_bytes = bytes(str(None), encoding='utf-8')
                    i64.from_bytes(none_to_bytes, 'big')
                    
                    literal.i64 = i64
                    print(dir(expression.Literal))
                    expression.literal.CopyFrom(literal)
                    
                    field_reference = expression.FieldReference()
                    root_reference = Expression.FieldReference.RootReference()
                    field_reference.root_reference.CopyFrom(root_reference)
                    expressions.append(expression)
        
    
    def visit_read(self, read_rel: ReadRel):
        print("visit_read")
        pass
        
    def visit_set(self, rel: SetRel):
        print("visit_set")
        
    
    def visit_sort(self, rel: SortRel):
        pass


union_visitor = UnionPlanVisitor(1)
visit_and_update(editor_1.rel, union_visitor)


def append_name_to_rel_root(substrait_plan, name:str):
    if substrait_plan.relations:
            relations = substrait_plan.relations
            if relations:
                if relations[0].HasField("root"):
                    rel_root = relations[0].root
                    if rel_root.names:
                        rel_root.names.append(name)

# when we take base_schema diff, we will know the missing field is `E`
append_name_to_rel_root(editor_1.plan, 'E')
# # The same we updated the plan with the expression, we should update the rel_root names
# # and think if we need to update the base_schema as well, but I don't think that is correct.
# ## APPROACH 2
print(editor_1.plan)
proto_bytes = editor_1.plan.SerializeToString()
query_result = con.from_substrait(proto=proto_bytes)
print(query_result.to_df())

# NOW WE HAVE AN AGREEMENT IN plan 1 and plan 2
# we can extract ReadRel from each plan and make a brand new plan with Set

# shutil.rmtree(test_dir) # should be the last line
set_rel = SetRel()
print(dir(set_rel))
print(set_rel.SET_OP_UNION_ALL)
set_rel.op = SetRel.SET_OP_UNION_ALL

extract_visitor_1 = ExtractTableVisitor()
visit_and_update(editor_1.rel, extract_visitor_1)

extract_visitor_2 = ExtractTableVisitor()
visit_and_update(editor_2.rel, extract_visitor_2)

project_rel_1 = extract_visitor_1.project_rel
project_rel_2 = extract_visitor_2.project_rel

read_rel_1 = extract_visitor_1.read_rel
read_rel_2 = extract_visitor_2.read_rel

print(type(set_rel.inputs))

rel1 = Rel()
rel1.project.CopyFrom(project_rel_1)

rel2 = Rel()
rel2.project.CopyFrom(project_rel_2)

#set_rel.inputs.extend
set_rel.inputs.append(rel1)
set_rel.inputs.append(rel2)

#set_rel.inputs.extend([read_rel_1, read_rel_2])
print("+" * 120)
#print(set_rel)

# print(dir(set_rel.inputs))

substrait_plan = SubstraitPlan()
print(dir(substrait_plan.relations))

plan_rel = PlanRel()
rel_root = RelRoot()
rel_root.names.append("A")
rel_root.names.append("B")
rel_root.names.append("C")
rel_root.names.append("D")
rel_root.names.append("E")

rel = Rel()
rel.set.CopyFrom(set_rel)


rel_root.input.CopyFrom(rel)
print(rel_root)
plan_rel.root.CopyFrom(rel_root)
substrait_plan.relations.append(plan_rel)

print("-" * 200)
print(substrait_plan)
print("-" * 200)


from google.protobuf.json_format import MessageToJson

json_obj = MessageToJson(substrait_plan)
print(json_obj)
# print("#" * 200)
# print(editor_1.plan)
# print("#" * 200)

substrait_json_plan = """
{
  "relations": [
    {
      "root": {
        "input": {
          "set": {
            "inputs": [
              {
                "project": {
                  "input": {
                    "read": {
                      "baseSchema": {
                        "names": [
                          "A",
                          "B",
                          "C",
                          "D",
                          "E",
                        ],
                        "struct": {
                          "types": [
                            {
                              "i64": {
                                "nullability": "NULLABILITY_NULLABLE"
                              }
                            },
                            {
                              "i64": {
                                "nullability": "NULLABILITY_NULLABLE"
                              }
                            },
                            {
                              "i64": {
                                "nullability": "NULLABILITY_NULLABLE"
                              }
                            },
                            {
                              "i64": {
                                "nullability": "NULLABILITY_NULLABLE"
                              }
                            },
                            {
                              "i64": {
                                "nullability": "NULLABILITY_NULLABLE"
                              }
                            }
                          ],
                          "nullability": "NULLABILITY_REQUIRED"
                        }
                      },
                      "projection": {
                        "select": {
                          "structItems": [
                            {},
                            {
                              "field": 1
                            },
                            {
                              "field": 2
                            },
                            {
                              "field": 3
                            }
                          ]
                        }
                      },
                      "localFiles": {
                        "items": [
                          {
                            "uriFile": "/home/asus/data/schema_evolution/advanced/d1.parquet",
                            "parquet": {}
                          }
                        ]
                      }
                    }
                  },
                  "expressions": [
                    {
                      "selection": {
                        "directReference": {
                          "structField": {}
                        },
                        "rootReference": {}
                      }
                    },
                    {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 1
                          }
                        },
                        "rootReference": {}
                      }
                    },
                    {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 2
                          }
                        },
                        "rootReference": {}
                      }
                    },
                    {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 3
                          }
                        },
                        "rootReference": {}
                      }
                    },
                    {
                      "literal": {
                        "i64": "0"
                      }
                    }
                  ]
                }
              },
              {
                "project": {
                  "input": {
                    "read": {
                      "baseSchema": {
                        "names": [
                          "A",
                          "B",
                          "C",
                          "D",
                          "E"
                        ],
                        "struct": {
                          "types": [
                            {
                              "i64": {
                                "nullability": "NULLABILITY_NULLABLE"
                              }
                            },
                            {
                              "i64": {
                                "nullability": "NULLABILITY_NULLABLE"
                              }
                            },
                            {
                              "i64": {
                                "nullability": "NULLABILITY_NULLABLE"
                              }
                            },
                            {
                              "i64": {
                                "nullability": "NULLABILITY_NULLABLE"
                              }
                            },
                            {
                              "i64": {
                                "nullability": "NULLABILITY_NULLABLE"
                              }
                            }
                          ],
                          "nullability": "NULLABILITY_REQUIRED"
                        }
                      },
                      "projection": {
                        "select": {
                          "structItems": [
                            {},
                            {
                              "field": 1
                            },
                            {
                              "field": 2
                            },
                            {
                              "field": 3
                            },
                            {
                              "field": 4
                            }
                          ]
                        }
                      },
                      "localFiles": {
                        "items": [
                          {
                            "uriFile": "/home/asus/data/schema_evolution/advanced/d2.parquet",
                            "parquet": {}
                          }
                        ]
                      }
                    }
                  },
                  "expressions": [
                    {
                      "selection": {
                        "directReference": {
                          "structField": {}
                        },
                        "rootReference": {}
                      }
                    },
                    {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 1
                          }
                        },
                        "rootReference": {}
                      }
                    },
                    {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 2
                          }
                        },
                        "rootReference": {}
                      }
                    },
                    {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 3
                          }
                        },
                        "rootReference": {}
                      }
                    },
                    {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 4
                          }
                        },
                        "rootReference": {}
                      }
                    }
                  ]
                }
              }
            ],
            "op": "SET_OP_UNION_ALL"
          }
        },
        "names": [
          "A",
          "B",
          "C",
          "D",
          "E"
        ]
      }
    }
  ]
}
"""

query_result = con.from_substrait_json(substrait_json_plan)
print(query_result)

