## Run Stanford CoreNLP
'''bash
java -mx8g -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer -annotators tokenize,ssplit,pos,ner -ner.model edu/stanford/nlp/models/ner/english.conll.4class.distsim.crf.ser.gz -ner.useSUTime false -ner.applyNumericClassifiers false -port 9000 -timeout 30000 -quiet > /dev/null 2>&1
'''

## Run Dbpedia Spotlight
'''bash
java -jar dbpedia-spotlight-latest.jar en http://localhost:9999/rest > /dev/null 2>&1
'''
