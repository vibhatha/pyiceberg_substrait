import logging
from icetrait.logging.logger import IcetraitLogger

icetrait_logger = IcetraitLogger(file_name="test.log")
logging.basicConfig(filename=icetrait_logger.log_path, encoding='utf-8', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Hello")
print(icetrait_logger.log_path)
with open(icetrait_logger.log_path, "r") as fp:
        for line in fp.readlines():
            print(line)
