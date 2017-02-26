from functools import partial
from bs4 import BeautifulSoup,Tag
import urllib
import SPARQLWrapper
import os
import json
import sys
from SPARQLWrapper.SPARQLExceptions import QueryBadFormed, EndPointNotFound
from stanza.nlp.corenlp import CoreNLPClient
from spotlight import annotate, SpotlightException

def dbpediaEntities(text, dbpedia_port='9999'):
    """
    Query dbpedia.
    """
    api = partial(annotate,
                  'http://localhost:'+ dbpedia_port + '/rest/annotate',
                  confidence=0.35,
                  support=10)
    try:
        results = api(text)
    # if no entities found
    except SpotlightException:
        results = []
    # any other error
    except:
        print("BAD REQUEST:DBPEDIA" , text)
        results = []

    entities = []
    for result in results:
        start = result['offset']
        name = result['surfaceForm']
        if type(name) != str:
            name = str(name)
        end = start + len(name)
        entities.append({
            'start' : start,
            'name' : name,
            'end' : end
            })
    return entities

def getFBmid(sparql, entity_link, name, retry_count=0):

    wiki_link = "http://en.wikipedia.org/wiki/"+ entity_link

    query= ('''prefix : <http://rdf.freebase.com/ns/>
            select distinct ?entity {
            ?entity <http://rdf.freebase.com/ns/common.topic.topic_equivalent_webpage> <'''+ wiki_link +"> \n"
            '''} LIMIT 100''')

    sparql.setQuery(query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)

    try:
        results = sparql.query().convert()
    except EndPointNotFound:
        if retry_count >= 3:
            print("FAIL REQUEST:WIKILINK" , wiki_link, name)
            return ''
        else:
            retry_count += 1
            return getFBmid(sparql, entity_link, name, retry_count=retry_count)
    except:
        print("BAD REQUEST:WIKILINK" , wiki_link, name)
        return ''

    if (len(results["results"]["bindings"]) >= 1): #should be a unique mid per page?
        result = results["results"]["bindings"][0]
        # mid found
        return result["entity"]["value"]
    else:
        # mid not found
        return ''

def getEntityLabels(sparql, fbmid, retry_count=0):
    labels = []
    query = ('''prefix : <http://rdf.freebase.com/ns/>
                select distinct ?entity_label 
            { <'''+ fbmid +'''> a ?entity_label
            } LIMIT 200''')

    sparql.setQuery(query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)

    try:
        results= sparql.query().convert()
    except EndPointNotFound:
        if retry_count >= 3:
            print("FAIL REQUEST:FBMID" , fbmid)
            return labels
        else:
            retry_count += 1
            return getEntityLabels(sparql, fbmid, retry_count=retry_count)
    except:
        print("BAD REQUEST:FBMID" , fbmid)
        return labels


    for result in results["results"]["bindings"]:
        # print("label: ",result["entity_label"]["value"])
        labels.append(result["entity_label"]["value"])
    return labels

def checkRelations(sparql, entities, retry_count=0):
    '''
    Queries for relations between all pairs of entities in given list. Called for every sentence.
    ''' 
    relations = []
    #take every combination of entities
    for e1 in entities:
        e1_fbmid = e1['fbmid']
        for e2 in entities:
            e2_fbmid = e2['fbmid']
            #skip entitites not in freebase
            if not e1_fbmid or not e2_fbmid or e1_fbmid == e2_fbmid:
                continue
            relation={}
            relation["e1"] = str(e1["start"]) + "_" + str(e1["end"])
            relation["e2"] = str(e2["start"]) + "_" + str(e2["end"])
            relation["labels"]=[]
            #query relations in freebase using mids of selected entities
            query=('''prefix : <http://rdf.freebase.com/ns/>\nselect distinct ?rel {'''
                    "<" + e1["fbmid"] + "> ?rel <" + e2["fbmid"] + ">\n} LIMIT 100")
            # print("Querying relation between "+e1["link"]+" , "+e2["link"])

            sparql.setQuery(query)
            sparql.setReturnFormat(SPARQLWrapper.JSON)
            try:
                results= sparql.query().convert()
            except EndPointNotFound:
                if retry_count >= 3:
                    print("FAIL REQUEST:RELATIONS " , entities)
                    return relations
                else:
                    retry_count += 1
                    return checkRelations(sparql, entities, retry_count=retry_count)
            except:
                print("BAD REQUEST:RELATIONS " , entities)
                return relations

            for result in results["results"]["bindings"]:
                # print("Relation :"+result["rel"]["value"])
                rel = result["rel"]["value"]
                relation["labels"].append(rel)

            if len(relation["labels"]) != 0:
                relations.append(relation)

    return relations



def process_file(input_file_path, sparql, corenlp_client, output_file_path):

    wiki_file = open(input_file_path, "r", encoding='utf-8')
    output_file = open(output_file_path, 'w', encoding='utf-8')
    
    for line in wiki_file:

        #end of an old doc
        if (line.startswith("</doc>")):
            continue

        #beginning of a new doc
        if(line.startswith("<doc")):
            soup = BeautifulSoup(line,"html.parser")
            doc_url = soup.contents[0]["url"]
            doc_name = soup.contents[0]["title"]
            doc_id = soup.contents[0]["id"]
            paragraph_count = 0
            continue

        if line == '\n':
            continue

        paragraph = {}
        soup = BeautifulSoup(line, "html.parser")
        para_string = ''
        offset = 0
        entities_fb = []
        paragraph = {}
        
        for content in soup.contents:
            string = content.string

            if not string:
                continue
            # replace ’ with '
            string = string.replace("’", "'")

            para_string += string

            #if string was contained inside a <a> tag, consider it to be an entity
            if content.name == "a":
                link = urllib.parse.unquote(content["href"])
                #note that these are relative urls
                #replace spaces in the url with underscores
                link = link.replace(' ', '_')
                name = string
                fbmid = getFBmid(sparql, link, name)
                if fbmid:
                    labels = getEntityLabels(sparql, fbmid)
                else:
                    labels = []
                #add entity attributes
                entities_fb.append({
                    'start': offset,
                    'end': offset + len(string),
                    'link': link,
                    'name': name,
                    'fbmid': fbmid, 
                    'labels': labels
                    })

            offset += len(string)
        
        # remove paragraphs that are very short
        if offset < 40:
            continue 
        # tokens, pos and ner tags
        annotated = corenlp_client.annotate(para_string)
        sentences = []
        sid = 0
        for sentence in annotated.sentences:
            tokens = []
            pos = []
            ner = []
            for token in sentence:
                tokens.append(token.word)
                pos.append(token.pos)
                ner.append(token.ner)
            sentences.append({
                'tokens' : tokens,
                'pos' : pos,
                'ner' : ner,
                'sid' : sid
                })
            sid += 1
        # Dbpedia annotations
        entities_db = dbpediaEntities(para_string)
        paragraph = {
                'fid' : doc_id,
                'fname' : doc_name,
                'furl' : doc_url,
                'pid' : paragraph_count,
                'text' : para_string,
                'fbEMs' : entities_fb,
                'dbEMs' : entities_db,
                'sentences' : sentences
                }
        paragraph_count += 1
        output_file.write(json.dumps(paragraph) + "\n")
    wiki_file.close()
    output_file.close()

''' Takes path to directory with folders and files extracted using WikiExtractor
    and path to output directory as inputs. Writes the sentences with required extracted information
    as Json files in output directory. 
    The Wikipedia dumps are extracted using the following command (preserve links)-

    python WikiExtractor.py -l enwiki-20160920-pages-articles-multistream.xml

'''

if __name__ == "__main__":
    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]

    corenlp_client = CoreNLPClient(server='http://localhost:9000', default_annotators=['ssplit', 'tokenize', 'lemma', 'pos', 'ner'])

    sparql = SPARQLWrapper.SPARQLWrapper("http://localhost:8890/sparql/")
    process_file(input_file_path, sparql, corenlp_client, output_file_path)
