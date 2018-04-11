##STKDATA
Simulation Tool Kit data file (stkdata) holds bulky chunked data (voltage recordings for example) related to one simulation.
It is also used to hold network structure, stimuli, etc.

It contains set of records and a footer. Each record has a structure:
```
#STKDATA          - magic prefix
2 bytes           - binary record header size '>H'
record header     - a text representation of the structure
 [
    int size,     - size of data
    int chunk,    - chunk number
    str datatype, - data type
    str variable  - name of variable
 ]
binary data       - data
```

A footer contains a tree of saved names and a binary tree size:
```
tree:
 "/name/name"
    [ (file,start,size,type),(file,start,size,type),(file,start,size,type)...]
                  - in the tree each record is a list of tuples
      file        - file name or None if data in current file
      start       - bites of offset to beginning of each chunk data
      size        - bites data size
      type        - data type
``` 
8 bytes           - binary size of tree '>Q'
