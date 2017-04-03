"""
Parse wiki data parsed by Wikiextractor to contever infolinks to DBpedia labels.
"""
import sys
import urllib
import json
from bs4 import BeautifulSoup
import SPARQLWrapper
from SPARQLWrapper.SPARQLExceptions import EndPointNotFound

def getDBpediaLabels(sparql, link, name, retry_count=0):
    """
    Get raw labels given by DBpedia.
    """

    query = ('''
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX dbo: <http://dbpedia.org/ontology/>
            SELECT ?labels WHERE {
                {
                    <http://dbpedia.org/resource/''' + link + '''> rdf:type ?labels
                }
            UNION
                {
                    <http://dbpedia.org/resource/''' + link + '''> dbo:wikiPageRedirects ?altName .
                    ?altName    rdf:type ?labels
                             
                }
            }

            ''')
    sparql.setQuery(query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)

    try:
        results = sparql.query().convert()
    except EndPointNotFound:
        if retry_count >= 3:
            print("FAIL REQUEST:WIKILINK", link)
            return None
        else:
            retry_count += 1
            return getDBpediaLabels(sparql, link, name, retry_count=retry_count)
    except:
        print("BAD REQUEST:WIKILINK", link)
        return None

    if len(results["results"]["bindings"]) >= 1: #should be a unique mid per page?
        result = results["results"]["bindings"]
        # mid found
        return result
    else:
        # mid not found
        print('No result found for ', link, name)
        return None

def parse_raw_labels(data):
    """
    Parse data and returns label and uri.
    """
    all_labels = []
    for return_dic in data:
        current_label = return_dic['labels']['value']
        if 'http://dbpedia.org/ontology/' in current_label:
            all_labels.append(current_label[len('http://dbpedia.org/ontology/'):])
    if not all_labels:
        all_labels.append('None')
    return all_labels

def process_file(input_file_path, sparql, output_file_path):
    """
    Process a single Wikiextractor output file.
    """

    wiki_file = open(input_file_path, "r", encoding='utf-8')
    output_file = open(output_file_path, 'w', encoding='utf-8')
    concept_annotated_in_kb, concept_not_annotated_in_kb = 0, 0
    good_para_count, bad_para_count, no_infolink_count = 0, 0, 0
    for line in wiki_file:

        #end of an old doc
        if line.startswith("</doc>"):
            continue

        #beginning of a new doc
        if line.startswith("<doc"):
            soup = BeautifulSoup(line, "html.parser")
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
        named_entities = []
        paragraph = {}
        bad_para = 0
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
                if link[0].islower():
                    link = link[0].upper() + link[1:]
                name = string
                raw_query_result = getDBpediaLabels(sparql, link, doc_id)
                if raw_query_result:
                    labels = parse_raw_labels(raw_query_result)
                    concept_annotated_in_kb += 1
                    # if concept is an entity mention
                    #add entity attributes
                    named_entities.append({
                        'start': offset,
                        'end': offset + len(string),
                        'name': name,
                        'link': link,
                        'labels': labels
                        })
                else:
                    concept_not_annotated_in_kb += 1
                    bad_para = 1
            offset += len(string)
        # remove paragraphs that are very short
        if offset < 40:
            continue

        # remove trailing \n
        if para_string[-1] == '\n':
            para_string = para_string[:-1]
        if bad_para:
            bad_para_count += 1
        else:
            if named_entities:
                good_para_count += 1
                paragraph = {
                    'fid' : doc_id,
                    'pid' : paragraph_count,
                    'text' : para_string,
                    'infoEMs' : named_entities,
                }
                output_file.write(json.dumps(paragraph) + "\n")
            else:
                no_infolink_count += 1
        paragraph_count += 1
    wiki_file.close()
    output_file.close()
    print(concept_annotated_in_kb, concept_not_annotated_in_kb)
    print(good_para_count, bad_para_count, no_infolink_count)

if __name__ == "__main__":
    process_file(sys.argv[1],
                 SPARQLWrapper.SPARQLWrapper("http://localhost:8890/sparql/"),
                 sys.argv[2])
