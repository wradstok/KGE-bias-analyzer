import gzip
import pathlib
import time
from typing import List
from entityparsers.entity import EntData

from entityparsers.manager import DataMgr
from helper import check_progress, read_lines, running_time

import argparse

if __name__ == '__main__':
    source = pathlib.Path(__file__).resolve().parent.parent.absolute()
    target = source.joinpath("data","latest-all.json.gz")

    # Initialize parser.
    data = DataMgr()
    parser = argparse.ArgumentParser(description="Wikidata entity parsing")
    parser.add_argument("parser", type=str, choices=data.get_parsers())
    parser.add_argument("--skip", dest="skip", type=int, help="# entities to skip", default=0)
    args = parser.parse_args()
    data.set_parser(args.parser)

    start_time = time.time() 
    iter_time = time.time()
    
    passed : int = 0
    with gzip.open(target) as f:
        # Skip the first x entities
        for i in range(args.skip):
            if i % 1_000_000 == 0:
                print(f"skipping: {i:,}")
            f.readline()

        lines = read_lines(f)
        while True:
            # Stop at the end of the file.
            if len(lines) == 0:
                break;

            # Process this batch
            entities : List[EntData] = list(filter(None, map(data.process_entity, lines)))
            data.add_entities({ent.id : ent for ent in entities})
            
            # Keep us posted every 100k.
            major, minor = check_progress(data, passed, iter_time, start_time)
            if minor:
                iter_time = time.time()
                passed += 1
            if major:
                passed = 0

            # Read the next lines.
            lines = read_lines(f)
        
    # Final dump.
    data.dump_current(data.selected_parser)

    # Final update.
    total_mins, total_secs = running_time(start_time)
    print(f"Finished running in {total_mins}:{total_secs}")
    print(f"Found {data.get_processed()} entities")



        