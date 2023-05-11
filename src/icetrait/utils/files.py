from urllib.parse import urlparse
from os.path import splitext, basename

def get_filename_and_extension(file_uri:str):
    disassembled = urlparse(file_uri)
    filename, file_ext = splitext(basename(disassembled.path))
    return filename, file_ext
