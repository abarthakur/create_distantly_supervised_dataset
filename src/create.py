import nltk
from nltk.tag.perceptron import PerceptronTagger
from bs4 import BeautifulSoup,Tag
import urllib
import SPARQLWrapper
import os
import json
import sys


def checkEntity(sparql,link,name):
    fbmid=""
    link2 = "http://en.wikipedia.org/wiki/"+link
    print(link2)
    print("querying ",link2)
    query= ('''prefix : <http://rdf.freebase.com/ns/>
            select distinct ?entity {
            ?entity <http://rdf.freebase.com/ns/common.topic.topic_equivalent_webpage> <'''+link2+"> \n"
            '''} LIMIT 100''')
    sparql.setQuery(query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    results= sparql.query().convert()
    if (len(results["results"]["bindings"])>=1):#should be a unique mid per page?
        result = results["results"]["bindings"][0]
        # print("mid: ",result["entity"]["value"])
        fbmid=result["entity"]["value"]
    else :
        # print("More than one, or less than one mid maps to this webpage. No. of mids : ",len(results["results"]["bindings"]))
        print("No mids found.")
        return ([],"")

    labels=[]
    query2=('''prefix : <http://rdf.freebase.com/ns/>
                select distinct ?entity_label 
            { <'''+fbmid+'''> a ?entity_label
            } LIMIT 100''')
    sparql.setQuery(query2)
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    try :
        results= sparql.query().convert()
    except Exception:
        print("Ill formed request")
        return ([],"")
    for result in results["results"]["bindings"]:
        # print("label: ",result["entity_label"]["value"])
        labels.append(result["entity_label"]["value"])
    return (labels,fbmid)


def checkRelations(sparql,entities):
    print("Checking for relations")
    relations=[]
    for e1 in entities:
        if e1["fbmid"]=="" :
            continue
        for e2 in entities:
            if e2["fbmid"]=="" :
                continue
            relation={}
            relation["e1"]=str(e1["start"])+"_"+str(e1["end"])
            relation["e2"]=str(e2["start"])+"_"+str(e2["end"])
            relation["labels"]=[]
            #query for which relations
            query=('''prefix : <http://rdf.freebase.com/ns/>\nselect distinct ?rel {'''
                    "<"+e1["fbmid"] +"> ?rel <"+e2["fbmid"]+">\n}")
            # print("Querying relation between "+e1["link"]+" , "+e2["link"])
            sparql.setQuery(query)
            sparql.setReturnFormat(SPARQLWrapper.JSON)
            try :
                results= sparql.query().convert()
            except Exception:
                print("Ill formed request")
                break
            for result in results["results"]["bindings"]:
                print("Relation :"+result["rel"]["value"])
                rel= result["rel"]["value"]
                relation["labels"].append(rel)
            if len(relation["labels"])!=0:
                relations.append(relation)
    return relations


def create(wiki_file_directory,target_directory,refresh):
    if not os.path.exists(target_directory):
        os.makedirs(target_directory)
    tagger = PerceptronTagger() 
    sparql = SPARQLWrapper.SPARQLWrapper("http://localhost:8890/sparql")
    subdirs=os.listdir(wiki_file_directory)
    stop_after=10
    for subdir in subdirs:
        if (os.path.isdir(wiki_file_directory+"/"+subdir)):
            wiki_files=os.listdir(wiki_file_directory+"/"+subdir)
            for file in wiki_files:
                f=open(wiki_file_directory+"/"+subdir+"/"+file,"r")
                print("Processing : "+wiki_file_directory+"/"+subdir+"/"+file+"\n\n\n\n")
                docline=f.readline()
                soup=BeautifulSoup(docline,"html.parser")
                docurl=soup.contents[0]["url"]
                docname=soup.contents[0]["title"]
                lineno=0
                sents=[]
                for line in f:
                    sent={}
                    lineno+=1
                    n=0
                    tokens=[]
                    tags=[]
                    entities=[]
                    relations=[]
                    soup = BeautifulSoup(line,"html.parser")
                    for content in soup.contents:
                        string=content.string
                        if not string:
                            continue
                        tok= nltk.word_tokenize(string)
                        tokens+=tok
                        if content.name =="a":
                            entity={}
                            link1= urllib.parse.unquote(content["href"])
                            chars=[]
                            for c in link1 :
                                if c==" ":
                                    c="_"
                                if c=="#":
                                    break
                                chars.append(c)
                            link="".join(chars)
                            print(link)
                            name=content.string
                            entity["start"]=n
                            entity["end"]=n+len(tok)
                            entity["link"]=link
                            entity["name"]=name
                            (entity["labels"],entity["fbmid"])=checkEntity(sparql,link,name)
                            entities.append(entity)
                        n+=len(tok)
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
                    # stop_after-=1
                    # if(stop_after==0):
                    #     break
                
                if not os.path.exists(target_directory+"/"+subdir):
                     os.makedirs(target_directory+"/"+subdir)
                with open(target_directory+"/"+subdir+"/"+file+".json","w") as jf:
                    json.dump(sents,jf)


if __name__ == "__main__":

    target_file_directory=sys.argv[2]
    wiki_file_directory = sys.argv[1]
    refresh=sys.argv[3] #not implemented
    create(wiki_file_directory,target_file_directory,refresh==1)