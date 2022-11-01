import json

def external_module(input_line):
    """ This is just an example function that reverses the lines fed to it."""
    if isinstance(input_line, bytes):
        print(input_line.decode()[::-1])
    else:
        print(input_line[::-1])

