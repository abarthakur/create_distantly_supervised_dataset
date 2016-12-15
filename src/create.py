import nltk
from nltk.tag.perceptron import PerceptronTagger
from bs4 import BeautifulSoup,Tag
import urllib
import SPARQLWrapper
import os
import json
import sys
from SPARQLWrapper.SPARQLExceptions import QueryBadFormed

mid_found=0
mid_not_found=0
no_sents=0
no_relations=0

def checkEntity(sparql,link,name):
    global mid_found
    global mid_not_found
    fbmid=""
    link2 = "http://en.wikipedia.org/wiki/"+link
    # print(link2)
    # print("querying ",link2)
    query= ('''prefix : <http://rdf.freebase.com/ns/>
            select distinct ?entity {
            ?entity <http://rdf.freebase.com/ns/common.topic.topic_equivalent_webpage> <'''+link2+"> \n"
            '''} LIMIT 100''')
    # print (query)
    sparql.setQuery(query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    try :
        results= sparql.query().convert()
    except SPARQLWrapper.SPARQLExceptions.QueryBadFormed:
        print("Bad Request")
        print (query)
        with open("badreqs.txt","a") as f:
            f.write(link2+"\n")
        return ([],"")
    if (len(results["results"]["bindings"])>=1):#should be a unique mid per page?
        result = results["results"]["bindings"][0]
        # print("mid: ",result["entity"]["value"])
        fbmid=result["entity"]["value"]
        print("mid found.")
        mid_found+=1
    else :
        # print("More than one, or less than one mid maps to this webpage. No. of mids : ",len(results["results"]["bindings"]))
        print("no mids found.")
        mid_not_found+=1
        return ([],"")

    labels=[]
    query2=('''prefix : <http://rdf.freebase.com/ns/>
                select distinct ?entity_label 
            { <'''+fbmid+'''> a ?entity_label
            } LIMIT 100''')
    sparql.setQuery(query2)
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    results= sparql.query().convert()

    for result in results["results"]["bindings"]:
        # print("label: ",result["entity_label"]["value"])
        labels.append(result["entity_label"]["value"])
    return (labels,fbmid)

'''Queries for relations between all pairs of entities in given list. Called for every sentence.''' 
def checkRelations(sparql,entities):
    global no_relations
    print("checking for relations")
    relations=[]
    #take every combination of entities
    for e1 in entities:
        #skip entitites not in freebase
        if e1["fbmid"]=="" :
            continue
        for e2 in entities:
            #skip entitites not in freebase
            if e2["fbmid"]=="" :
                continue
            relation={}
            relation["e1"]=str(e1["start"])+"_"+str(e1["end"])
            relation["e2"]=str(e2["start"])+"_"+str(e2["end"])
            relation["labels"]=[]
            #query relations in freebase using mids of selected entities
            query=('''prefix : <http://rdf.freebase.com/ns/>\nselect distinct ?rel {'''
                    "<"+e1["fbmid"] +"> ?rel <"+e2["fbmid"]+">\n}")
            # print("Querying relation between "+e1["link"]+" , "+e2["link"])
            sparql.setQuery(query)
            sparql.setReturnFormat(SPARQLWrapper.JSON)
            results= sparql.query().convert()
            for result in results["results"]["bindings"]:
                # print("Relation :"+result["rel"]["value"])
                rel= result["rel"]["value"]
                relation["labels"].append(rel)
            if len(relation["labels"])!=0:
                relations.append(relation)
                no_relations+=1
    return relations


''' Takes path to directory with folders and files extracted using WikiExtractor
    and path to output directory as inputs. Writes the sentences with required extracted information
    as Json files in output directory. 
    The Wikipedia dumps are extracted using the following command (preserve links)-

    python WikiExtractor.py -l enwiki-20160920-pages-articles-multistream.xml

'''
def create(wiki_file_directory,target_directory,skip):
    global no_sents
    stopper=0
    if not os.path.exists(target_directory):
        os.makedirs(target_directory)
    tagger = PerceptronTagger() 
    sparql = SPARQLWrapper.SPARQLWrapper("http://localhost:8890/sparql/")
    print("processing "+wiki_file_directory)
    for subdir in os.listdir(wiki_file_directory):
        wiki_sub_directory=wiki_file_directory+"/"+subdir
        if (os.path.isdir(wiki_sub_directory)):
            print("processing "+wiki_sub_directory)
            wiki_files=os.listdir(wiki_file_directory+"/"+subdir)
            for file in wiki_files:
                wiki_file_path=wiki_sub_directory+"/"+file
                f=open(wiki_file_path,"r")
                print("processing "+wiki_file_path)
                #the first line describes the document
                docline=f.readline()
                soup=BeautifulSoup(docline,"html.parser")
                docurl=soup.contents[0]["url"]
                docname=soup.contents[0]["title"]
                #begin processing the sentences in the document
                lineno=0
                sents=[]
                for line in f:
                    stopper+=1
                    print(stopper)
                    #skip "skip" no of lines
                    if(stopper < skip):
                        continue
                    lineno+=1
                    sent={}
                    tokens=[]
                    entities=[]
                    word_position=0
                    soup = BeautifulSoup(line,"html.parser")
                    for content in soup.contents:
                        string=content.string
                        #skip empty strings
                        if not string:
                            continue
                        #tokenize the content of the tag
                        toks= nltk.word_tokenize(string)
                        tokens+=toks
                        #if string was contained inside a <a> tag, consider it to be an entity
                        if content.name =="a":
                            entity={}
                            link1= urllib.parse.unquote(content["href"])
                            print("url1 : "+link1)
                            chars=[]
                            #replace spaces in the url with underscores
                            #note that these are relative urls
                            for character in link1 :
                                if character==" ":
                                    character="_"
                                chars.append(character)
                            link="".join(chars)
                            print("url2 : " + link)
                            name=content.string
                            #add entity attributes
                            entity["start"]=word_position
                            entity["end"]=word_position+len(toks)
                            entity["link"]=link
                            entity["name"]=name
                            (entity["labels"],entity["fbmid"])=checkEntity(sparql,link,name)
                            entities.append(entity)
                        word_position+=len(toks)

                    tags = tagger.tag(tokens)
                    tags2=[]
                    for tag in tags:
                        tags2.append(tag[1])
                    sent["sentid"]=lineno
                    sent["tokens"]=tokens
                    sent["tags"]=tags2
                    sent["mentions"]=entities
                    sent["relations"]=checkRelations(sparql,entities)
                    sent["docname"]=docname
                    sent["docurl"]=docurl
                    sents.append(sent)
                if not os.path.exists(target_directory+"/"+subdir):
                    os.makedirs(target_directory+"/"+subdir)
                with open(target_directory+"/"+subdir+"/"+file+".json","w") as jf:
                    json.dump(sents,jf)
    no_sents=stopper


if __name__ == "__main__":

    target_file_directory=sys.argv[2]
    wiki_file_directory = sys.argv[1]
    skip=int(sys.argv[3]) #not implemented
    create(wiki_file_directory,target_file_directory,skip)
    with open("stats.txt","w") as f :
        f.write("No of Sentences :"+no_sents)
        f.write("No of entities linked to freebase :"+mid_found)
        f.write("No of entities not found in freebase :"+mid_not_found)
        f.write("No of relations :"+no_relations)
