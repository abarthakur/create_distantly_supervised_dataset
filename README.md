## Project Overview

This project contains code to annotate the Wikipedia Corpus with DBPedia. It takes doc files created by ``wikiextractor`` and queries all hyperlinked words in it to check if they are entities. Thereafter checks all pairs of entities in a sentence by querying if they have relations between them.

## Usage

1. Download XML dumps of Wikipedia from [here](https://dumps.wikimedia.org/) and place in data/raw
2. Install ``WikiExtractor.py`` from [the repo](https://github.com/attardi/wikiextractor).
3. Extract the dump into individual files using ```WikiExtractor.py`` like this (-l to preserve links)

```
python WikiExtractor.py -l enwiki-20160920-pages-articles-multistream.xml
```
4. Run StanfordCoreNLP and DBPedia Spotlight servers.
5. Run ``create.py`` with directory containing output of Step 3 as input

```
python create.py ../data/raw ../data/processed/json_output
```

## Run Stanford CoreNLP
'''bash
java -mx8g -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer -annotators tokenize,ssplit,pos,ner -ner.model edu/stanford/nlp/models/ner/english.conll.4class.distsim.crf.ser.gz -ner.useSUTime false -ner.applyNumericClassifiers false -port 9000 -timeout 30000 -quiet > /dev/null 2>&1
'''

## Run Dbpedia Spotlight
'''bash
java -jar dbpedia-spotlight-latest.jar en http://localhost:9999/rest > /dev/null 2>&1
'''
