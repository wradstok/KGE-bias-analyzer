# Bias Profiling

All code required to reproduce the results is in the `code` folder.  Specifically, analysis consists of two parts:

1. Extract a relevant subset and properties from the wikidata JSON dump.
2. Analyse this subset for the topic of interest.

The first step is done by utilizing the python script preset under `code/entityparsers`. Currently there are five entity parsers. Three of these collect subsets from Wikidata. The other two collect auxillary information.

1. Full: Fetch every item in wikidata. We collect how often We collect how often each each possible value type is present (see [here](https://www.wikidata.org/wiki/Help:Wikidata_datamodel/)), and how often individual (predicate,object) combinations occur. Additionally, we store in how many languages the description, name of the item are present.
2. Human: fetches everything from the human class. I.e. any items which is an *instance of* (P31) *human* (Q5). We store again how often each value type is present and the (predicate, object) occurrences.
3. Human_temp: fetches everything from the human class, where that human has at least one statements with a temporal qualifier, or at least one fact which is directly temporal (e.g. *date of birth *(P569)).  We store the occurrences of the (predicate, object) combinations, directly temporal string and temporal qualifiers.
4. Country: fetch everything that looks like a 'place'. These can be two things. Firstly, items that directly have the property *country* (P17). Secondly, items that have the property *located in the administrative territorial entity* (P171). We store just the country or the administrative territory identifiers.
5. Labels: fetch all entities and their english name (if present, otherwise the first name listed).

These parsers expect that the Wikidata JSON dump is present in the `data` folder. This dump can be downloaded [here](https://dumps.wikimedia.org/wikidatawiki/entities/). Use 'latest-all.json.gz'. The data does not need to be unpacked: we operate on the compressed file to save space. To run a parser, navigate to the `code\entityparsers` subfolder and run:

```
python3 wikidata_parser.py [name-of-parser]
```

This will start the process of reading the Wikidata JSON dump, collecting any entities and their relevant properties according to the selected parser. Note that this will take a long time: around 10-20 hours in our case. Regular updates will be written to your console. Wikidata contains around 90 million entities. Every 1 million, the script will append the current progress to a pickle file called `[selected-parser].p`.  

The second step is done with the help of a Jupyter notebook. 