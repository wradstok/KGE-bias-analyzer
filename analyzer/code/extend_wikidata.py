"""
This script extends the Wikidata12k dataset with occupation triples for all the humans
it contains. Additionally, it adds more women to the dataset so that the number of 
men and women in the 5 most common occupations is equal (Physicist, Writer, Actor,
Association football player, Politician). To run it, a file 'wd_woman_triple.txt' which
contains these extra triples is required to be present in the 'metadata' folder. This file
can be created by running the code in the bias-profiling repository. 
"""

import pandas as pd
from helper import (
    get_path,
    get_random_state,
    call_wikidata_api,
    check_num_outlinks,
    write_gen_occ,
)

location = ["data", "wikidata12k"]

# Read in train, test validation sets.
data = pd.DataFrame([], columns=["s", "p", "o", "tb", "te"])
sets = ['train.txt', 'test.txt','valid.txt']
for d_set in sets:
    data = pd.concat([data, pd.read_table(
        get_path(*location, "raw", d_set),
        header=None,
        names=["s", "p", "o", "tb", "te"],
    )], axis=0, ignore_index=True)

data = data.drop(['tb', 'te'], axis=1)
data = data.drop_duplicates(ignore_index=True)

# Convert ids to Wikidata ids.
ents =  pd.read_csv(get_path(*location, "raw", "entities.txt"), sep="\t", header=None, names=['id', 'name'])
rels =  pd.read_csv(get_path(*location, "raw", "relations.txt"), sep="\t", header=None, names=['id', 'name'])

ent_dict = {}
for idx, row in ents.iterrows():
    ent_dict[row.id] = row['name']

rel_dict = {}
for idx, row in rels.iterrows():
    rel_dict[row.id] = row['name']

data['p'] = data.p.apply(lambda x: rel_dict[x])
data['s'] = data.s.apply(lambda x: ent_dict[x])
data['o'] = data.o.apply(lambda x: ent_dict[x])

random_state = get_random_state()

genders = {}
occupations = {}

# 1) Extend wikidata dataset by adding occupation triples for all the men/women that are
# already in the dataset. Wikidata12k originally does not have P106.
all_entities = data.s.append(data.o).unique()
query_group = []
for i in range(len(all_entities)):
    query_group.append(all_entities[i])
    if i == len(all_entities) - 1 or len(query_group) == 50:
        result = call_wikidata_api(query_group, ["P106", "P21"])
        for ent, entdata in result.items():
            if "P21" in entdata:
                genders[ent] = entdata["P21"]
            if "P106" in entdata:
                occupations[ent] = entdata["P106"]
        query_group = []  # Reset query
        print(f"Queried {i} out of {len(all_entities)} entities.")

occ_triples = []
for ent, occ in occupations.items():
    occ_triples.append([ent, "P106", occ])
data = pd.concat([data, pd.DataFrame(occ_triples, columns=["s", "p", "o"])])
data = data.sample(frac=1).reset_index(drop=True)
data.to_csv(get_path(*location, "original", "triples.txt"), sep="\t", header=False, index=False)

missing_women = {occupation : 0 for occupation in set(occupations.values())} 
for ent, occ in occupations.items():
    gender = genders.get(ent)
    if gender == "Q6581097":
        missing_women[occ] += 1
    elif gender == "Q6581072":
        missing_women[occ] -= 1

# 2) Extend wikidata dataset by loading extra women triples from the subset we extracted.
# This set was already created in the other project, so all that remains doing here is to
# add them & do some gender/occupation bookkeeping.
extra_triples = pd.read_table(
    get_path("metadata", "wd_woman_triples.txt"),
    header=None,
    names=["s", "p", "o"],
)

for idx, row in extra_triples[extra_triples.p == "P106"].iterrows():
    genders[row.s] = "Q6581072"
    occupations[row.s] = row.o

# Create new balanced_df by adding the extra female triples. Then shuffle it.
balanced_df = pd.concat([data, extra_triples]).reset_index(drop=True)
balanced_df = balanced_df.sample(frac=1).reset_index(drop=True)
balanced_df.to_csv(
    get_path(*location, "extended", "triples.txt"), index=False, sep="\t", header=None
)

write_gen_occ(occupations, genders, location)

all_men = {k for k, v in genders.items() if v == "Q6581097"}
all_women = {k for k, v in genders.items() if v == "Q6581072"}

print("In the balanced dataset:")
check_num_outlinks(balanced_df, all_men, all_women)

print("Versus in the original dataset:")
check_num_outlinks(data, all_men, all_women)

# In the balanced dataset:
# avg outlinks for men: 5.79 vs. for women: 4.85
# avg inlinks for men: 0.10 vs. for women: 0.06
#
# Versus in the original dataset:
# avg outlinks for men: 5.79 vs. for women: 4.14
# avg inlinks for men: 0.10 vs. for women: 0.30

