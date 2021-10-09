from typing import Dict, List, Optional
from entityparsers.manager import EntData

class Women(EntData):
    def __init__(self, id: str):
        self.id = id

        self.track : Dict[str, List[str]] = { 
            'P6' : [], 'P17' : [], 'P26' : [], 'P27' : [], 'P31' : [],
            'P39' : [], 'P54' : [], 'P69' : [], 'P102' : [], 'P106': [],
            'P108' : [], 'P131' : [], 'P150' : [], 'P166' : [], 
            'P190' : [], 'P463' : [], 'P512' : [], 'P551' : [],
            'P579' : [], 'P793' : [], 'P1346' : [], 'P1376' : [], 
            'P1411' : [], 'P1435' : [], 'P2962' : [],
        }
        
    def process(self, data : Dict) -> EntData:
        statements : Optional[Dict] = data.get('claims') 
        if statements is None:
            return None

        # If we are not human, exit.
        instances_of = statements.get('P31')
        if instances_of is None or 'Q5' not in map(self.claim_object, instances_of):
            return None

        sex_gender = statements.get('P21')
        if sex_gender is None or 'Q6581072' not in  map(self.claim_object, sex_gender):
            return None
        
        for statement, claims in statements.items():
            if statement in self.track: 
                for claim in claims:
                    snak = claim['mainsnak']
                    claim_type = self.get_claim_type(snak)
    
                    # Add the entity referred to the correct tracking list.
                    if claim_type == "wikibase-entityid" and snak['snaktype'] == 'value':
                        self.track[statement].append(snak['datavalue']['value']['id'])

        return self