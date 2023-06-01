import logging
import os

class IcetraitLogger:
    
    def __init__(self, file_name="icetrait.log") -> None:
        self._log_file_path = None
        log_dir = os.getenv("ICETRAIT_LOG_DIR")
        do_log = os.getenv('ICETRAIT_LOGGING')
        if do_log == 'ENABLE':
            if not log_dir:
                print("log directory environment variable `ICETRAIT_LOG_DIR` not set.")
                print(f"Creating Log directory at path {log_dir}")
                os.mkdir("icetrait_logs")
                print(f"Log directory created at path {log_dir}")
            if not os.path.exists(log_dir):
                print(f"Log directory `{log_dir}` already exists")
                os.mkdir(log_dir)
            self._log_file_path = os.path.join(log_dir, file_name)
        elif do_log == 'DISABLE':
            print("Logging disabled")
       
    @property 
    def log_path(self):
        return self._log_file_path
