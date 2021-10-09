"""
This script modifies/extends the DBPedia NIF dataset. Regarding modification, we
remove all predicates which occur less than 50 times in the dataset. Next, we query DBPedia 
for every entity in the KG to check whether it is of a specific `type`, of we have 49. 
These are direct descendants of the class 'Person' in DBPedia, + the class Person itself.
Once we have this subset of entities of these classes, we query those that have a link
to Wikidata, on Wikidata. This is the case for about 7904 out of 9180. 
From Wikidata we get the occupation and gender information. We can get this for 7856 people.
We add these triples to the KG, this becomes the 'original' graph.

"""

import pandas as pd
import requests
import pickle
from helper import (
    get_path,
    get_random_state,
    call_wikidata_api,
    write_gen_occ,
    check_num_outlinks,
    get_gender_balance,
)
from typing import List, Dict
from collections import Counter
from random import sample

DBPEDIA_URL_START = (
    "https://dbpedia.org/sparql?default-graph-uri=http://dbpedia.org&query="
)
DBPEDIA_URL_END = "&format=application%2Fsparql-results%2Bjson&timeout=60000&signal_void=on&signal_unconnected=on"


def call_dbpedia_query(entities: List[str], entity_types: List) -> Dict:
    """
    This humongous thing is the SPARQL query used to fetch all the 'people' from
    DBPedia that are in the given categories. Strangely enough, occupations like PlayboyPlaymate
    or RomanEmperor are highly listed.

    They can be obtained using:
        SELECT *
        WHERE
        {
            ?item rdfs:subClassOf dbo:Person
        }
    """
    query_start = (
        DBPEDIA_URL_START
        + 'SELECT DISTINCT ?id ?label ?wdLink (GROUP_CONCAT(DISTINCT ?type; SEPARATOR=", ") AS ?types)'
        + " WHERE { ?id rdf:type ?type. ?id rdfs:label ?label ."
        + " FILTER (?type IN ( "
        +  requests.utils.quote(",".join(entity_types))
        + " ) )"
        + " FILTER (?label IN ( "
    )  # "Anky van Grunsven"@en, "Emilia Fox"@en, "Ross Barkley"@e ... entities go here
    query_end = (
        ") )"
        + ' OPTIONAL { ?id owl:sameAs ?wdLink . FILTER CONTAINS(STR(?wdLink),"wikidata") } }'
        + " GROUP BY ?id ?label ?wdLink"
        + DBPEDIA_URL_END
    )

    # First convert all to labels.
    ent_strings = []
    for entity in entities:
        ent = entity.split("/resource/").pop()
        ent = ent.replace("_", " ")
        ent_strings.append(ent)

    ent_string = ",".join(list(map(lambda x: f'"{x}"@en', ent_strings)))
    ent_string = requests.utils.quote(ent_string)
    query = query_start + ent_string + query_end

    r = requests.get(url=query)
    r_data = r.json()

    results = {}
    for result in r_data["results"]["bindings"]:
        # Only people are actually returned. So we can assume this result is a person.
        person = {k: v["value"] for k, v in result.items()}
        results[person["id"]] = person

    return results

def lookup_wd_occ_gender(people : Dict, genders: Dict, occupations: Dict, wd_db_map: Dict, filename: str):
    """ Look-up peoples occupation and gender on Wikidata. `people` argument should
     be the DBpedia entity data. Variables are edited in place.
    """

    # Try reading from file first.
    local_file = get_path("metadata", f"{filename}.p")
    if local_file.is_file():
        with open(local_file, "rb") as f:
            data = pickle.load(f)
            genders = data['genders']
            occupations = data['occupations']
            wd_db_map = data['wd_to_db']
            return genders, occupations, wd_db_map

    query_group = []
    for i, dbpedia_ent in enumerate(people.values()):
        wd_id = dbpedia_ent["wdLink"].split("/entity/").pop()
        wd_db_map[wd_id] = dbpedia_ent["id"]
        query_group.append(wd_id)

        if i == len(people) - 1 or len(query_group) == 50:
            result = call_wikidata_api(query_group, ["P106", "P21"])
            for ent, entdata in result.items():
                if "P21" in entdata:
                    genders[wd_db_map[ent]] = entdata["P21"]
                if "P106" in entdata:
                    occupations[wd_db_map[ent]] = entdata["P106"]
            query_group = []  # Reset query
            print(f"Queried {i} out of {len(people)} entities.")
    
    # Write output to file.
    with open(local_file, "wb") as f:
        pickle.dump({'genders': genders,'occupations': occupations, 'wd_to_db': wd_db_map }, f)
    
    return genders, occupations, wd_db_map


def count_balance(from_df, gender_info: Dict):
    """`from_df` dataframe consisting only of (`person`, db_occupation, `occupation`)
        triples. """ 
    # Count the number of occurrences for each occupation, split out over genders.
    occupation_genders = {
        occ: {gender: 0 for gender in set(gender_info.values())}
        for occ in from_df.o.unique()
    }
    for _, row in from_df.iterrows():
        if row.o in occupation_genders and row.s in gender_info:
            gender = gender_info[row.s]
            occupation_genders[row.o][gender] += 1

    # So how many people do we need to fetch?
    # Yes, we are being biased here! Only counting men and women
    # even though there are several people who identify as another gender.
    missing = {
        occ: counts["Q6581097"] - counts["Q6581072"]  # men - women
        for occ, counts in occupation_genders.items()
    }

    return missing

###
###
### START OF SCRIPT
###
###
location = ["data", "dbpedia"]
data = pd.read_table(
    get_path(*location, "raw", "triples.txt"), header=None, names=["s", "p", "o"]
)
data = data.drop_duplicates(ignore_index=True)
random_state = get_random_state()

# Remove all predicates which occur less than 50 times.
preds_to_remove = set(
    map(lambda x: x[0], filter(lambda x: x[1] < 50, data.p.value_counts().iteritems()))
)

removed_triples = data[data.p.isin(preds_to_remove)]
print(f"Removing {len(preds_to_remove)} predicates and {len(removed_triples)} triples.")
data = data.drop(removed_triples.index, axis=0).reset_index(drop=True)
print(f"Leaves {len(data.p.unique())} predicates and {len(data)} triples.")


# Fetch metadata from DBpedia if we don't have it yet.
entity_data = {}
entities = data.s.append(data.o).unique()
metadata_file = get_path("metadata", "dbpedia_entitydata.p")

# Load all entity types that we want to fetch.
with open(get_path('metadata', 'dbpedia_persontypes.txt'), 'r') as f:
    entity_types = f.read().splitlines()

if not metadata_file.is_file():
    query_group = []
    print("Fetching metadata from dbpedia.")
    for i in range(len(entities)):
        query_group.append(entities[i])
        # Query 100 entities at a time.
        if i == len(entities) - 1 or len(query_group) == 100:
            result = call_dbpedia_query(query_group, entity_types)

            entity_data.update(result)

            query_group = []  # Reset query
            print(f"Queried {i} out of {len(entities)} entities.")
    # Dump to file.
    with open(metadata_file, "wb") as f:
        pickle.dump(entity_data, f)
else:
    # Read from file.
    with open(metadata_file, "rb") as f:
        print("Reading wikidata info from file.")
        entity_data = pickle.load(f)

# For these people we found, we still need to fetch their gender.
# We can do this from wikidata, as long as they had a sitelink.
linked_to_wd = {k: v for k, v in entity_data.items() if "wdLink" in v}
print(f"{len(entity_data)} persons, with {len(linked_to_wd)} linked to wikidata.")

genders, occupations, wd_to_db = lookup_wd_occ_gender(linked_to_wd, {}, {}, {}, 'dbpedia_wd_query1')

# Use the dbpedia occupation types to augment the dataset with occupation triples.
# Base on linked_to_wd, as these are guaranteed to be people.
db_occupations = []
for db_data in linked_to_wd.values():
    types = map(lambda x: x.split("/ontology/").pop(), db_data["types"].split(","))
    types = [x for x in types if x != "Person"] # We're not interested in the fact that they are a person.
    if len(types) > 0:  # Append first type, if it exists.
        db_occupations.append([db_data["id"], "db_occupation", "http://dbpedia.org/resource/" + types[0]])

# Shuffle the triples in and write to file.
db_occupations = pd.DataFrame(db_occupations, columns=data.columns)
data = pd.concat([data, db_occupations]).reset_index(drop=True)
data = data.sample(frac=1).reset_index(drop=True)
data.to_csv(
    get_path(*location, "original", "triples.txt"), index=False, sep="\t", header=None
)

# So.. with the 'original' data taken care of, we now need to fetch some extra women.
most_common_occupations = Counter(db_occupations.o).most_common()
# The 5 most common are.
# [('OfficeHolder', 2508), ('Athlete', 1436), ('Royalty', 1002), ('SportsManager', 288), ('Scientist', 282)]

all_men, all_women = get_gender_balance(set(data.s.append(data.o)), genders, "original dataset")  # 6767 men, 1087 women
missing = count_balance(db_occupations, genders)

# Remark about a path which didn't work.
#  DBPedia has 241 predicates which apply to people
#   SELECT *
#   WHERE {
#       ?item rdfs:domain dbo:Person
#   }
# In our dataset there are 205 predicates. Intersecting them with the 241 gives
# {'deathPlace', 'spouse', 'placeOfBurial', 'ethnicity', 'nationality', 'child',
#   'parent', 'birthPlace', 'allegiance', 'residence', 'almaMater'}
# However, this does not work since we are confusing DBpedia ontology with the actual
# predicates applied in this dataset, which are not neccesarily part of the ontology.
# It causes us to miss items like 'doctoralStudents', which is applied to people.
# Instead, we say:
all_people = all_women.union(all_men)
people_preds = list(set(data[data.s.isin(all_people)].p.unique()).union(
    set(data[data.o.isin(all_people)].p.unique()))
) # which is 112 predicates (ignoring 'db_occupation').
people_preds = [x for x in people_preds if x != 'db_occupation']


# Python dictionaries maintain order from python 3.6.
# We shorten the names to identifiers because otherwise the query becomes too large
# for a GET request, and the DBPEDIA sparql interface does not seem to support POST requests.
pred_names = list(map(lambda x: x.split('/property/').pop(), people_preds))
pred_names = {item : i for i, item in enumerate(pred_names)}

# Furthermore, we fetch only item + wdLink + 18 OPTIONALS at a time, because we are only allowed 20
# predicates (keys) at most. This means we need 7 queries to fetch all data for a single item.
local_file = get_path("metadata", "dbpedia_extra_data.p")
if not local_file.is_file():
    # If someone occurs as 2 types, e.g. militaryperson and janitor
    # we store only the occurrence we found last. 
    additional_info = {}
    pred_ids, pred_groups = [], []
    local, prev = "", 0
    for i, (item, item_id) in enumerate(pred_names.items()):
        if i != 0 and (i % 18 == 0 or i == len(pred_names) - 1):
            pred_groups.append(local)
            pred_ids.append(f" ".join(map(lambda x: '?' + str(x), range(prev, i))))
            local, prev = "", i
        local += f" OPTIONAL {{ ?item dbp:{item} ?{item_id} }} "

    # Perform actual querying.
    for persontype, missing_amount in missing.items():
        p_type = persontype.split("/resource/").pop()
        if missing_amount < 100:
            continue
        print(f"Fetching information for type {p_type}")

        for i in range(len(pred_groups)):
            query = f"SELECT DISTINCT ?item ?wdLink " + pred_ids[i]
            query += " WHERE { "
            query += f" ?item rdf:type dbo:{p_type} . "
            query += " ?item owl:sameAs ?wdLink ."
            query += " FILTER CONTAINS(STR(?wdLink),'wikidata') "
            query += pred_groups[i]
            query += "} GROUP BY ?item ?wdLink"
            query += f" LIMIT {missing_amount* 3}" # With a buffer, because there are more men than women.

            url = DBPEDIA_URL_START +  requests.utils.quote(query) + DBPEDIA_URL_END

            r = requests.get(url=url)
            r_data = r.json() 

            q_results = r_data['results']['bindings']
            for q_res in q_results:
                name = q_res['item']['value']
                if name not in additional_info:
                    additional_info[name] = {
                        'id' : name,
                        'wdLink' : q_res['wdLink']['value'],
                        'occupation' : p_type
                        }
                
                for k,v in q_res.items():
                    if k == 'item' or k == 'wdLink': # We handled it above.
                        continue
                    pred = people_preds[int(k)] # Map predicate back from id to name.
                    additional_info[name][pred] = v['value'] 
    with open(local_file, "wb") as f:
        pickle.dump(additional_info, f)
else:
    with open(local_file, "rb") as f:
        print("Reading dbpedia information from file.")
        additional_info = pickle.load(f)

# Yay, we have some additional items. We now need to need to process this.
# 1) Filter out the people that we already have in the dataset.
# 2) Query Wikidata again for occupations and genders
# 3) Determine which of these people we are going to add.
duplicates = {x for x in additional_info.keys() if x in all_people}
additional_info = {k : v for k,v in additional_info.items() if k not in duplicates}
print(f"{len(duplicates)} people found were duplicates")

# Step 2:
genders, _, wd_to_db_new = lookup_wd_occ_gender(additional_info, genders, {}, {}, 'dbpedia_wd_query2')
new_men, new_women = get_gender_balance(set(wd_to_db_new.values()), genders, "newly found data")

# Count for each entity how many predicates it has which object value is already in the dataset.
women_scores = {k : {} for k in missing.keys()}
entity_set = set(entities)
occupations = {row.s : row.o for i, row in db_occupations.iterrows()}
for woman in new_women:
    w_data = additional_info[woman]
    num_existing = sum([1 for value in w_data.values() if value in entity_set])
    occupation = "http://dbpedia.org/resource/" + w_data['occupation']
    women_scores[occupation][woman] = num_existing
    occupations[woman] = occupation

additional_triples = []
for occupation, women_scores_ranked in women_scores.items():
    women_scores_ranked = {k for (k, _) in Counter.most_common(women_scores_ranked)} # Only store the labels
    
    for i, woman in enumerate(women_scores_ranked):
        if i >= missing[occupation]:
            print(f"Stopped adding for {occupation} because we had enough.")
            break
        w_data = additional_info[woman]
        for k,v in w_data.items():
            if k not in ['id', 'wdLink', 'occupation', 'name']:
                name =  str(v if 'http' in v else "http://dbpedia.org/resource/" + v).replace(" ", "_")
                additional_triples.append([woman, k, name])
        additional_triples.append([woman,'db_occupation', occupation])

additional_triples = pd.DataFrame(additional_triples, columns=data.columns)
missing_now = count_balance(additional_triples[additional_triples.p == 'db_occupation'], genders)
print(missing_now)


balanced_data = pd.concat([data, additional_triples]).reset_index(drop=True)
balanced_data = balanced_data.sample(frac=1).reset_index(drop=True)

all_men, all_women = get_gender_balance(set(balanced_data.s.append(balanced_data.o)), genders, "balanced dataset 1")

# Let's try to remove some men.
men_to_keep_count = int(len(all_women) / len(all_men) * len(all_men))
men_to_remove = all_men.difference(set(sample(all_men, men_to_keep_count)))

print("Let's remove some men:")
balanced_data = balanced_data[(~balanced_data.s.isin(men_to_remove)) & (~balanced_data.o.isin(men_to_remove))]
all_men, all_women = get_gender_balance(set(balanced_data.s.append(balanced_data.o)), genders, "balanced dataset 2")

balanced_data.to_csv(
    get_path(*location, "extended", "triples.txt"), index=False, sep="\t", header=None
)


print("In the balanced dataset:")
check_num_outlinks(data, all_men, all_women)
print("Versus in the original dataset:")
check_num_outlinks(balanced_data, all_men, all_women)

# Let's save all the occupations/genders we have to file.
write_gen_occ(occupations, genders, location)

# In the balanced dataset:
# avg outlinks for men: 7.14 vs. for women: 5.12
# avg inlinks for men: 2.91 vs. for women: 2.83
# Versus in the original dataset:
# avg outlinks for men: 6.70 vs. for women: 2.83
# avg inlinks for men: 2.58 vs. for women: 0.51