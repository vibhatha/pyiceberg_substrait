from substrait.gen.proto.plan_pb2 import Plan

class ProcessSubstrait:
    
    def __init__(self, plan_path:str):
        self._path = plan_path
