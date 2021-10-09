from typing import Dict, Optional

from entityparsers.manager import EntData

class FullEnt(EntData):

    def __init__(self, id: str):
        self.id = id

        # Claims/statement data.
        self.object_mentions : Dict[str, int] = {}
        self.claim_counts : Dict[str, int] = {
            'wikibase-entityid' : 0, # This is the # of subject occurrences
            'quantity': 0,
            'string': 0,
            'time': 0,
            'globecoordinate': 0,
            'monolingualtext': 0,
            'multilingualtext': 0
        }

        # Sitelink data
        self.sitelink_count = 0
        self.badges_count = 0
        self.badges_unique = 0

        # Other
        self.description_count = 0
        self.label_count = 0

    def process(self, data: Dict) -> Optional[EntData]:
        if(data.get('claims') != None):
            self.process_claims(data['claims'])

        if(data.get('descriptions') != None):
            self.process_descriptions(data['descriptions'])
        
        if(data.get('labels') != None):
            self.process_labels(data['labels'])
        
        if(data.get('sitelinks') != None):
            self.process_sitelinks(data['sitelinks'])

        return self

    def process_claims(self, claims: Dict) -> None:
        for statements in claims.values():
            for claim in statements:
                snak = claim['mainsnak']
                claim_type = self.get_claim_type(snak)

                if claim_type != "unknown":
                    self.claim_counts[claim_type] += 1   
        
                if claim_type == "wikibase-entityid":
                    id = snak['datavalue']['value']['id']
                    self.object_mentions[id] = self.object_mentions.get(id, 0) + 1 

                
    def process_descriptions(self, descriptions: Dict) -> None:
        self.description_count = len(descriptions)

    def process_labels(self, labels: Dict) -> None:
        self.label_count = len(labels)

    def process_sitelinks(self, sitelinks: Dict) -> None:
        self.sitelink_count = len(sitelinks)

        links_with_badges = list(filter(lambda x: len(x['badges']) > 0, sitelinks.values()))
        all_badges = [item for sublist in links_with_badges for item in sublist['badges']]
        self.badges_count = len(all_badges)
        self.badges_unique = len(set(all_badges))