import pickle

from typing import Dict, List, Optional
from functools import partial

from entityparsers.entity import EntData
from entityparsers.full import FullEnt
from entityparsers.human import Human
from entityparsers.human_def import Human_def
from entityparsers.human_temp import TempHuman
from entityparsers.labels import Labels
from entityparsers.country import Country
# from entityparsers.women import Women

class DataMgr():

    def __init__(self):
        self.entities : Dict[str, EntData] = {}
        self.processed = 0
        self.saved = 0

        self.parsers = {
            "full" : partial(FullEnt),
            "human" : partial(Human),
            "label": partial(Labels),
            "human_temp": partial(TempHuman),
            "country" : partial(Country),
            "human_def": partial(Human_def),
            # "women": partial(Women),
        }
        self.selected_parser : str = ""

    def get_parsers(self) -> List[str]:
        return list(self.parsers.keys())

    def set_parser(self, parser: str) -> None:
        self.selected_parser = parser
        self.parser = self.parsers[parser]

    def process_entity(self, ent_data: Dict) -> Optional[EntData]:
        self.processed += 1
        entity = self.parser(ent_data['id'])
        entity = entity.process(ent_data)
        if entity is not None:
            self.saved += 1
        return entity
    
    def dump_current(self, name: str):
        """ Append currently processed entities to file & wipe entities """ 
        with open(f"{name}.p", "ab") as f:
            pickle.dump(self.entities, f)
        self.entities = {}
        
    def add_entities(self, entities : Dict[str, EntData]) -> None:
        self.entities.update(entities)

    def get_size(self) -> int:
        return self.saved

    def get_processed(self) -> int:
        return self.processed
