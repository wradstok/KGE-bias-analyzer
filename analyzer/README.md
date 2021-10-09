To get started, you must have Python (64 bit) 3.6 or 3.7 installed. Older or newer versions unfortunately do not work due to dependency conflicts. Next, you can install the required dependencies by running the following in your shell.

> py -m pip install -r requirements.txt

The data folder contains 3 subdirectories for every dataset (i.e., wikidata12k and dbpedia NIF). The original datasets should be placed in the `raw` folder. When analysis is run, it will firstly be converted into a proper format and some rows will be dropped. The result is placed in the `original` folder. Afterwards, balancing is applied automatically. The result of the balancing is placed in the `extended` folder.

Once the raw data has been placed in the correct directory, balancing scripts can be run with: 

> py extend_wikidata.py
> py extend_dbpedia.py

Once balancing has been performed, the `run_ampli.py` script will train an embedding model. The result will be placed in the `experiments` folder. Arguments to the script are:

> py run_ampli.py <dataset> <version> <model>

Where dataset is either *wikidata12k* or *dbpedia*, version is *original* or *extended* and model is *transe*. Since the embeddings are trained from scratch, this will take some time depencing on your compute performance. 

Finally, we can perform an analysis on the trained model. Arguments here are the same as for the `run_ampli` script. I.e:

> py analyze.py <dataset> <version> <model>

The results of the analysis will be written to file.