from typing import Dict, List, Optional, Union
from entityparsers.manager import EntData
from abc import abstractmethod

class Fact:
    def __init__(self,  references: Optional[List]):
        self.references : List[Reference] = []

        # Try to parse and references for this fact.
        if references is None:
            return
        
        for reference in references:
            self.references.append(Reference(reference))

    @abstractmethod
    def get_name(self) -> str:
        pass

class TimeFact(Fact):
    def __init__(self, snak: Dict, references: Optional[List]):
        values = snak['datavalue']['value']
        if values.get('calendarmodel') != "http://www.wikidata.org/entity/Q1985727":
            # If not "gregorian proleptic calendar", store what it is.
            self.calendar = values.get('calendarmodel').split("/").pop()
        
        self.timestamp = values['time']
        self.precision = values['precision']

        super().__init__(references)

    def get_name(self):
        return "temporal"


class EntityFact(Fact):
    def __init__(self, snak: Dict, references: Optional[List]):
        self.object = snak['datavalue']['value']['id']
        super().__init__(references)
    
    def get_name(self):
        return "entity"
     

class CoordFact(Fact):
    def __init__(self, snak: Dict, references: Optional[List]):
        super().__init__(references)

    def get_name(self):
        return "coord"



def parse_snak(snak: Dict, references: Optional[List] ) -> Optional[Fact]:
    """Parse a claim in addition to its references""" 
    if snak['snaktype'] != "value":
        return None

    datatype = snak.get('datatype')

    if datatype == "wikibase-item":
        return EntityFact(snak, references)
    elif datatype == "time":
        return TimeFact(snak, references)
    elif datatype == "coordinate":
        return CoordFact(snak, references)
    else:
        return None

class Reference:
    """ A reference can consist of multiple predicates, and for each
        predicate there can be multiple entries."""
    def __init__(self, ref_snaks: Dict):
        self.items : Dict[str, List[Fact]] = {}

        references : Optional[Dict] = ref_snaks.get('snaks')
        if references is None:
            return None
        
        # Every predicate can have one more statements (snaks) associated with it.
        for pred, snak_list in references.items():
            for snak in snak_list:
                # Lets assume there are no references to our references.
                ref = parse_snak(snak, None)
                if ref is not None:
                    sub = self.items.get(pred, [])
                    sub.append(ref)
                    self.items[pred] = sub

class Human_def(EntData):
    def __init__(self, id: str):
        self.id = id
        
        self.tracking : Dict[str, Dict[str, List[Fact]]] = {
            'entity' : {},
            'temporal' : {},
            'coord'   : {},
        }

    def process(self, data : Dict) -> EntData:
        self.languages = list(data.get('labels', {}).keys())

        statements : Optional[Dict] = data.get('claims') 
        if statements is None:
            return None

        # If we are not human, exit.
        instances_of = statements.get('P31')
        if instances_of is None or 'Q5' not in map(self.claim_object, instances_of):
            return None
        
        for pred_id, claims in statements.items(): 
            for claim in claims:
                fact = parse_snak(claim.get('mainsnak'), claim.get('references'))
                if fact is None:
                    continue
                
                # Store back in either objects/temporal data.
                target = self.tracking[fact.get_name()]
                sub = target.get(pred_id, [])
                sub.append(fact)
                target[pred_id] = sub

        return self