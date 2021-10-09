from typing import Dict
from entityparsers.manager import EntData

class Labels(EntData):
    # Fetch EN label for every entity
    # This for our own easy of use...
    def __init__(self, id: str):
        self.id = id
        self.label = ""        

    def process(self, data : Dict) -> EntData:
        labels = data.get("labels") 
        if labels is not None and len(labels) > 0:
            en = labels.get("en")
            if en is None:
                self.label = labels[list(labels.keys())[0]]
            else:
                self.label = en["value"]

        return self

# How to parse the format:
# Load all entity labels
#start_time = time.time()
# i, ent_labels = 0, {}
# source = open(Path(os.path.abspath('')).resolve().joinpath("label.p"), "rb")
# count = 0
# with open(Path(os.path.abspath('')).resolve().joinpath("labels.txt"), "w") as f:
#     while True:   
#         batch = next_batch(source)
#         if batch is None:
#             break
#         count += len(batch)
#         for id, entity in batch.items():
#             label = entity.label
#             if type(entity) == Dict:
#                 continue
#             f.write(f"{entity.id}:{label}\n")
#         del batch
#         i += 1
#         if i % 25 == 0:
#             print(f"Parsed {count:,} labels in {i:,} batches. Took {time.time() - start_time} seconds")
# source.close()