import pickle
import os
from pathlib import Path
from typing import Dict, List, Tuple, Set
import heapq
import pandas as pd

# Required to un-pickle the file.
# from entityparsers.women import Women

PredId = str
EntId = str

Count = Tuple[int, EntId]
Heap = List[Count]

def get_path(*path_elements):
    src = Path(os.path.abspath('')).resolve()
    for item in path_elements:
        src = src.joinpath(item)
    return src

def next_batch(file):
    try:
        return pickle.load(file)
    except:
        return None

def parse_batch(batch, curr_data):
    for id, subject in batch.items():
        # Keep only the predicates with actual values.
        data = { predicate : obj_list for predicate, obj_list in subject.track.items() if len(obj_list) > 0 } 
        curr_data[id] = data

if __name__ == '__main__':
    women : Dict[EntId, Dict[PredId, List[EntId]]] = {} # Contains the raw data.
    with open(get_path('code', 'women.p'), 'rb') as source:
        while True:
            batch = next_batch(source)
            if batch is None:
                break
            
            parse_batch(batch, women)
        print(f"Parsed {len(women)} women")
    
    # Read the entities which currently exist in Wikidata12k.
    with open(get_path('code','metadata', 'wikidata12k_ents.txt'), 'r') as f:
        existing_ents : Set[EntId] = set(f.read().split(','))

    women_heap : Dict[EntId, Heap] = {} 
    women_filtered : Dict[EntId, Dict[PredId, List[EntId]]] = {} # Contains only the objects we already had.
    for id, claims in women.items():
        # Filter the list of objects by the existing entities.
        filtered_copy : Dict[PredId, List[EntId]] = {}
        for pred, objects in claims.items():
            filtered_copy[pred] = list(filter(lambda x: x in existing_ents, objects))

        # Store occupation in full.
        occupations = claims.get('P106', [])
        filtered_copy['P106'] = occupations
        
        if len(occupations) > 0:
            first_occ = occupations[0]
            # Based only on the number of unique predicates present.
            pair =  -len(claims), id  # Maxheap!

            if first_occ not in women_heap:
                women_heap[first_occ] = [] # Initialize
            heapq.heappush(women_heap[first_occ], pair)

            women_filtered[id] = filtered_copy

    # For now, we are actually only interested in the following 5 occupations:
    # [football player, politician, actor, writer, physicist]
    common_occupations : Set[EntId] = set(['Q937857', 'Q82955', 'Q33999', 'Q36180', 'Q169470'])
    required = {'Q937857': 1850, 'Q82955': 750 , 'Q33999': 50, 'Q36180': 150, 'Q169470': 150}

    output_ids : List[EntId] = []
    for occupation in common_occupations:
        relevant_women = women_heap[occupation]

        for i in range(0, required[occupation]):
            count, woman_id = heapq.heappop(relevant_women)
            output_ids.append(woman_id)

    output_data : List = []
    for id in output_ids:
        woman = women_filtered[id]
        for pred, obj_list in woman.items():
            if pred == "P31":
                continue # We are not interested in the fact that they are human.
            for obj in obj_list:
                output_data.append([id, pred, obj])

    women_df = pd.DataFrame(output_data)
    women_df.to_csv(get_path("woman_triples.csv"), sep="\t", header=False, index=False)
        




        
    
