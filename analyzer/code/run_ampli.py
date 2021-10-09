from posixpath import abspath
import subprocess
import sys
import argparse
import os
import math
import numpy as np

from pathlib import Path
from typing import Dict

from ampligraph.datasets import load_from_csv
from ampligraph.latent_features import TransE, ComplEx
from ampligraph.evaluation import (
    evaluate_performance,
    mr_score,
    mrr_score,
    hits_at_n_score,
    train_test_split_no_unseen
)
from ampligraph.utils import save_model, restore_model

def init_model(name, batch_count:int ):
    if name == 'transe':
        return TransE(
            k=100,
            optimizer="adam",
            batches_count=batch_count,
            optimizer_params={"lr": 0.001},
            loss="self_adversarial",
            eta=500,
            epochs=200,
            verbose=True,
        )


def evaluate(model, test, others) -> Dict[str, float]:
    ranks = evaluate_performance(
        test,
        model=model,
        filter_triples=np.concatenate((train, others)),
        corrupt_side="s,o",
        filter_unseen=False,
        verbose=True,
    )

    result = {
        "mrr": mrr_score(ranks),
        "mr": mr_score(ranks),
        "hits_1": hits_at_n_score(ranks, n=1),
        "hits_3": hits_at_n_score(ranks, n=3),
        "hits_10": hits_at_n_score(ranks, n=10),
    }

    for metric, value in result.items():
        print(f"{metric}: {value:.3}")

    return result


if __name__ == "__main__":
    subprocess.check_call(
        [sys.executable, "-m", "pip", "-q", "install", "tensorflow==1.15.0"]
    )
    subprocess.check_call(
        [sys.executable, "-m", "pip", "-q", "install", "ampligraph==1.4.0"]
    )

    parser = argparse.ArgumentParser(description="KG embedding model training/testing script.")
    parser.add_argument("dataset", type=str, help="Name of the dataset to be imported")
    parser.add_argument(
        "version", type=str, help="Name of the version of the dataset being investigated."
    )

    parser.add_argument("model", type=str, help='Name of the model to use')

    parser.add_argument(
        "--test",
        action="store_true",
        help="Skip training: just load an existing model and test it",
        default=False,
    )
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

    data = load_from_csv(source_dir, "triples.txt")
    train, test = train_test_split_no_unseen(data, test_size=0.2, seed=23891367)
    print(f"Train size: {len(train)}")
    print(f"Test size: {len(test)}")

    batch_count = int(math.ceil(len(train) / 250))
    model_path = experiment_dir.joinpath(f"{args.model}-model.amp")
    if args.test:
        model = restore_model(model_path)
    else:
        model = init_model(args.model, batch_count)
        model.fit(train)
        save_model(model, model_path)
    
    evaluate(model, test, train)