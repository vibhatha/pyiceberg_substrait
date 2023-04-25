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
        
        
