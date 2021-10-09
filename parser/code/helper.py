from gzip import GzipFile
from typing import List, Tuple
import time
import json

from entityparsers.manager import DataMgr

def running_time(start):
    diff = time.time() - start
    mins = diff / 60
    secs = diff % 60
    return int(mins), int(secs)

def read_lines(file : GzipFile) -> List:
    if file.closed:
        return []
    
    bytelines = file.readlines(3 * int(1E8)) # Read 300MB, this will expand to ~3GB

    # Parse the bytelines in to json. We remove ",\n" chars from all the lines
    # and the first & last line from the document ('[', ']') by filtering length.
    strlines = []
    for line in bytelines:
        utf8_line = line.decode('utf-8')
        if len(utf8_line) > 2:
            strlines.append(json.loads(utf8_line[:-2]))

    return strlines

def check_progress(data: DataMgr, passed: int, iter_time, start_time) -> Tuple[bool, bool]:
    saved = data.get_size()
    
    major = False
    minor = int(saved / 100_000) > passed
    if minor:
        iter_mins, iter_secs = running_time(iter_time) 
        total_mins, total_secs = running_time(start_time)
        print(f"{data.get_processed():,} entities processed, {saved:,} saved. Last iter in {iter_mins}:{iter_secs}, total time {total_mins}:{total_secs} ")

    # Dump every ~1m entities
    if passed != 0 and passed % 10 == 0 :
        major = True
        print("Starting dump")
        data.dump_current(data.selected_parser)
        print("Finished dump")
        
    return major, minor