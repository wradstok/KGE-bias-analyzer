from typing import Dict, List, Optional
from entityparsers.manager import EntData

class TemporalData:
    def __init__(self, obj_entity: str, claim: Dict):
        self.entity = obj_entity
        self.temp_pred = claim['property']            

        values = claim['datavalue']['value']
        if values.get('calendarmodel') != "http://www.wikidata.org/entity/Q1985727":
            # If not "gregorian proleptic calendar", store what it is.
            self.calendar = values.get('calendarmodel').split("/").pop()
        
        self.timestamp = values['time']
        self.precision = values['precision']

class TempHuman(EntData):

    def __init__(self, id: int):
        self.id = id

        self.temp_preds : List[TemporalData] = []
        self.indirect : Dict[str, List[TemporalData]] = {} 


    def process(self, data : Dict) -> EntData:
        statements : Optional[Dict] = data.get('claims') 
        if statements is None:
            return None

        # If we are not human, exit.
        instances_of = statements.get('P31')
        if instances_of is None or 'Q5' not in map(self.claim_object, instances_of):
            return None
                
        temporal_info_found = False
        for pred, claims in statements.items():
            for claim in claims:
                snak = claim['mainsnak']
                claim_type = self.get_claim_type(snak)

                if claim_type == "time":
                    temporal_info_found = True 
                    self.temp_preds.append(TemporalData("", snak))
                
                # Only store qualifiers for actual (s,p,o) triples
                if claim_type != "wikibase-entityid":
                    continue

                # Process qualifiers.
                qualifiers: Optional[Dict[str, List]] = claim.get('qualifiers')
                if qualifiers is None:
                    continue

                for q_pred, q_claims in qualifiers.items():
                    for q_claim in q_claims:
                        if q_claim['datatype'] != 'time' or q_claim['snaktype'] != 'value':
                            continue # Ignore non temporal qualifiers
                        
                        temporal_info_found = True
                        temp_data = TemporalData(snak['datavalue']['value']['id'], q_claim)
                        track = self.indirect.get(pred, [])
                        track.append(temp_data)
                        self.indirect[pred] = track

        if temporal_info_found:
            self.languages = list(data.get('labels', {}).keys())
            return self
        return None