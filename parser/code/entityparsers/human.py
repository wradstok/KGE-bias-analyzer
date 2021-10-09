from typing import Dict, List, Optional
from entityparsers.manager import EntData

class Human(EntData):
    # There should be ~9.062.359 humans in Wikidata 12/04/2021
    # 7660 professions
    # 40 types of actors

    def __init__(self, id: str):
        self.id = id
        
        # Wikidata defines 177 properties to be applied to people
        # 38 of which are considered protected for living people (i.e,)
        # https://www.wikidata.org/wiki/Wikidata:Property_proposal/living_people_protection_class
        self.tracking : Dict[str, List[str]] = {}
        self.claim_counts : Dict[str, int] = {
            'wikibase-entityid' : 0, # This is the # of subject occurrences
            'quantity': 0,
            'string': 0,
            'time': 0,
            'globecoordinate': 0,
            'monolingualtext': 0,
            'multilingualtext': 0,
            "unknown": 0,
        }

        self.languages : List[str] = []

    def process(self, data : Dict) -> EntData:
        self.languages = list(data.get('labels', {}).keys())

        statements : Optional[Dict] = data.get('claims') 
        if statements is None:
            return None

        # If we are not human, exit.
        instances_of = statements.get('P31')
        if instances_of is None or 'Q5' not in map(self.claim_object, instances_of):
            return None
        
        for statement, claims in statements.items(): 
            for claim in claims:
                snak = claim['mainsnak']
                claim_type = self.get_claim_type(snak)
                self.claim_counts[claim_type] += 1   
    
                # Add the entity referred to the correct tracking list.
                if claim_type == "wikibase-entityid":
                    track = self.tracking.get(statement, [])
                    track.append(snak['datavalue']['value']['id'])
                    self.tracking[statement] = track

        return self