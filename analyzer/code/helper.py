from typing import Dict, Optional, Set, List, Tuple
from pathlib import Path
import os
import requests


def get_gender_balance(all_ents: Set, genders: Dict, what: str) -> Tuple[Set, Set]:
    all_men = {k for k in all_ents if genders.get(k) == "Q6581097"}
    all_women = {k for k in all_ents if genders.get(k) == "Q6581072"}
    print(f"{what} contains {len(all_men)} men and {len(all_women)} women.")

    return all_men, all_women

def read_wd_claim(pred_claims: Optional[Dict]):
    if pred_claims is None:
        return None
    if pred_claims[0]["mainsnak"]["snaktype"] != "value":
        return None
    return pred_claims[0]["mainsnak"]["datavalue"]["value"]["id"]


def call_wikidata_api(entity_ids: List[str], predicates: List[str]):
    url = "https://www.wikidata.org/w/api.php?action=wbgetentities&ids="

    target = url + "|".join(entity_ids) + "&format=json"
    r = requests.get(url=target)
    r_data = r.json()

    entity_data: Dict[str, Dict[str, str]] = {}
    for id, ent in r_data["entities"].items():
        claims = ent.get("claims")

        if claims is None:
            continue  # Somehow occurs for 'Q29051963' -> is removed from WD

        # Check that we are actually operating on a person: (X, P31, Q5)
        instances_of = claims.get("P31")
        if instances_of is None or read_wd_claim(instances_of) != "Q5":
            continue

        entity_data[id] = {}
        for pred in predicates:
            ent_data = claims.get(pred)
            claim_data = read_wd_claim(ent_data)
            entity_data[id][pred] = claim_data if claim_data is not None else "unknown"

    return entity_data


def write_gen_occ(occupations: Dict, genders: Dict, location: List):
    """Write gender/occupation to file."""
    entities = set(occupations.keys()).union(set(genders.keys()))

    with open(get_path(*location, "humans.txt"), "w", encoding="utf-8") as f:
        for entity in entities:
            # We're not guaranteed to have these, since they only make sense on humans.
            gender, occ = genders.get(entity, ""), occupations.get(entity, "")
            if gender != "" or occ != "":
                f.write(f"{entity}\t{gender}\t{occ}\n")


def check_num_outlinks(df, men: Set, women: Set):
    male_sub_only = df[df.s.isin(men)]
    male_obj_only = df[df.o.isin(men)]

    female_sub_only = df[df.s.isin(women)]
    female_obj_only = df[df.o.isin(women)]

    num_women = len(female_sub_only.s.append(female_obj_only.o).unique())
    num_men = len(male_sub_only.s.append(male_obj_only.o).unique())

    print(
        f"avg outlinks for men: {male_sub_only.s.value_counts().sum() / num_men:.2f} vs. for women: {female_sub_only.s.value_counts().sum() / num_women:.2f}"
    )
    print(
        f"avg inlinks for men: {male_obj_only.o.value_counts().sum() / num_men:.2f} vs. for women: {female_obj_only.o.value_counts().sum() / num_women:.2f}"
    )


def get_random_state():
    return 331929


def get_occupation_mapping():
    return {
        "Q937857": "association football player",
        "Q82955": "politician",
        "Q33999": "actor",
        "Q36180": "writer",
        "Q169470": "physicist",
    }


def get_gender_mapping():
    return {"Q6581097": "Male", "Q6581072": "Female"}


def get_path(*path_elements):
    src = Path(os.path.abspath(os.getcwd())).resolve()
    for item in path_elements:
        src = src.joinpath(item)
    return src


def get_labels(human, predicate, only_first):
    results = []
    if human.get("claims") is None:
        return []
    claims = human["claims"].get(predicate)
    if claims is None:
        return []

    for claim in claims:
        if claim["mainsnak"]["snaktype"] == "value":
            results.append(claim["mainsnak"]["datavalue"]["value"]["id"])
            if only_first:
                break
    return results


def order_dict(dictionary):
    return {
        k: v for k, v in sorted(dictionary.items(), key=lambda x: x[1], reverse=True)
    }
