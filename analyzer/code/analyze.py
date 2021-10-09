from pathlib import Path
import argparse
import os
import pandas as pd
from ampligraph.datasets.datasets import load_from_csv
from ampligraph.evaluation import train_test_split_no_unseen
from ampligraph.utils import restore_model
from ampligraph.discovery import query_topn


def calc_men_women(entities, genders, occupation_mapping, occupation):
    # Determine how many of each entities are of each gender, when they have the given occupation.
    people_with_occ = [person for person in entities if occupation_mapping.get(person) == occupation]
    men_with_occupation = [person for person in people_with_occ if genders.get(person) == 'Q6581097']
    women_with_occupation = [person for person in people_with_occ if genders.get(person) == 'Q6581072']
    non_people = [ent for ent in entities if ent not in set(occupation_mapping.keys())]

    return men_with_occupation, women_with_occupation, non_people


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze a model")
    parser.add_argument("dataset", type=str, help="Name of the dataset to be analyzed")
    parser.add_argument(
        "version", type=str, help="Name of the version of the dataset being analyzed."
    )
    parser.add_argument("emb_model", type=str, help="Name of the embedding model used (e.g. TransE, ComplEx)")
    parser.add_argument("occupation_predicate", type=str, help="Identifier of the occupation predicate. E.g. 'P106' for wikidata.")

    args = parser.parse_args()

    source_dir = (
        Path(os.path.abspath(""))
        .resolve()
        .parent.joinpath("data", args.dataset, args.version)
    )

    experiment_dir = (
        Path(os.path.abspath(""))
        .resolve()
        .parent.joinpath("experiments", args.dataset, args.version)
    ) 

    model = restore_model(experiment_dir.joinpath(args.emb_model + "-model.amp"))
    gender_mapping, occ_mapping = {}, {}
    with open(source_dir.parent.joinpath("humans.txt"), "r", encoding="utf-8") as f:
        lines = f.read().splitlines()
        for line in lines:
            items = line.split("\t")
            if len(items) == 3 and items[2] != "":
                ent, gender, occupation = items
                gender_mapping[ent] = gender
                occ_mapping[ent] = occupation

    data = load_from_csv(source_dir, "triples.txt")
    data_df = pd.DataFrame(data, columns=["s", "p", "o"])
    data_occ = data_df[data_df.p == args.occupation_predicate]

    # Utilize the same seed as in run_ampli.py so we generate the same train/test split.
    train, _ = train_test_split_no_unseen(data, test_size=0.2, seed=23891367)
    train_df = pd.DataFrame(train, columns=['s', 'p', 'o'])
    train_occ = train_df[train_df.p == args.occupation_predicate]
  
    # # Entities that have an occupation listed.
    train_entities = train_occ.s.unique()
    all_entities = data_occ.s.unique()

    query_results = {}
    for occupation in set(train_occ.o):  # Only use occupations actually in the dataset.
        triples, scores = query_topn(
            model,
            100,
            head=None,
            relation=args.occupation_predicate,
            tail=occupation,
            ents_to_consider=list(train_entities), # Not casting to list gives a ValueError. 
            # We allow any entity that was in the training set.
            # This includes entities that are not people.
        )
        
        genders = [gender_mapping.get(person, 'UNKNOWN') for person in triples[0:, 0]]
        query_results[occupation] = {"triples": triples, "scores": scores, "genders": genders}

    # Write results to file for analysis.
    for occupation, results in query_results.items():
        occ_name = occupation.split('/resource/').pop()
        with open(experiment_dir.joinpath(f"occ-{occ_name}.txt"), "w") as f:
            # Total number of men and women
            men, women, none = calc_men_women(all_entities, gender_mapping, occ_mapping, occupation)
            f.write(f"There are {len(men)} men and {len(women)} women {occ_name} in the dataset.\n")

            # Write how many of each gender occur in each occupation.
            men, women, none = calc_men_women(train_entities, gender_mapping, occ_mapping, occupation)
            f.write(f"There are {len(men)} men and {len(women)} women {occ_name} in the train set.\n")
            f.write(f"There are {len(none)} entities which are not man or woman. \n")

            # Write top-100 query result genders to file.
            for res in results['genders']:
                f.write(f"{res}\n")