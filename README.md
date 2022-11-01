# State Reader #
State reader is a module and a stand alone CLI tool that tracks the state of files that it processes.
An external module parses the file line by line.  
If the program crashes, it will re start at the last position in the file.  

External modules can be used if you want to ingest data into databases, translate large files and so on.

### External modules ###  
Python imports modules in a rather unwieldy way, since it is extremely hard to import modules from files in another part of the filesystem.  
The BETA solution for now is to place a file called "external.py" with a function called external_module inside the modules directory.  
```from modules import external.external_module```  
If you know a better way, please let me know.  

### Usage ###
```
usage: statereader.py [-h] -f FILE [-b BYTES] [-s STATEFILE] [-t] [-m]

optional arguments:
  -h, --help            show this help message and exit
  -f FILE, --file FILE  File to get processed
  -b BYTES, --bytes BYTES
                        Bytestream position, which byte to jump to
  -s STATEFILE, --statefile STATEFILE
                        State file where bytestream is stored
  -t, --tail            Tail the file continuously
  -m, --module          external.py with module name external is placed in modules directory

```