from typing import Dict, List, Optional
from entityparsers.manager import EntData

class Country(EntData):
    def __init__(self, id: str):
        self.id = id
        self.country :str = "unknown"
        self.territories : List[str] = []

    def process(self, data : Dict) -> EntData:
        statements : Optional[Dict] = data.get('claims') 
        if statements is None:
            return None

        # If we are not human, exit.
        country_claims = statements.get('P17')
        territorial_claims = statements.get('P131')
        
        # Try looking at territorial claims
        if country_claims is None:
            if territorial_claims is None:
                return None
            for claim in territorial_claims:
                snak = claim['mainsnak']
                if self.get_claim_type(snak) == "wikibase-entityid":
                    self.territories.append(snak['datavalue']['value']['id'])
        else:   
            for claim in country_claims:
                # Only save the first country
                snak = claim['mainsnak']
                if self.get_claim_type(snak) == "wikibase-entityid":
                    self.country = snak['datavalue']['value']['id']
                return self

        return self

