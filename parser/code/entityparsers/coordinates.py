from typing import Dict, Optional
from entityparsers.manager import EntData

class Coordinates(EntData):

    def __init__(self, id: str):
        self.id = id

        self.coords : str = ""
        self.num_triples : int = 0

    def process(self, data : Dict) -> EntData:
        statements : Optional[Dict] = data.get('claims') 
        if statements is None:
            return None

        # Check if a coordinate predicate 
        coord_claims = statements.get('P625')
        if coord_claims is None:
            return None
        
        for statement, claims in statements.items(): 
            for claim in claims:
                self.num_triples += 1

                snak = claim['mainsnak']
                claim_type = self.get_claim_type(snak)
    
                # Add the entity referred to the correct tracking list.
                if claim_type == "wikibase-entityid":
                    track = self.tracking.get(statement, [])
                    track.append(snak['datavalue']['value']['id'])
                    self.tracking[statement] = track

        return self