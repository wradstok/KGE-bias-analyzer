from abc import ABC, abstractmethod
from typing import Dict, Optional

class EntData(ABC):
    
    def __init__(self, id: str):
        self.id = id

    @staticmethod
    def get_claim_type(snak : Dict) -> str:
        if snak['snaktype'] != 'value':
            return "unknown"
        return snak['datavalue']['type']

    @staticmethod
    def claim_object(claim: Dict) -> Optional[str]:
        if claim['type'] == 'statement' and claim['mainsnak']['snaktype'] == 'value':
            return claim['mainsnak']['datavalue']['value']['id']
        return None

    @abstractmethod
    def process(self, data : Dict) -> 'EntData':
        # https://stackoverflow.com/questions/33533148/how-do-i-type-hint-a-method-with-the-type-of-the-enclosing-class
        # Gemini cluster runs Python 3.6
        pass