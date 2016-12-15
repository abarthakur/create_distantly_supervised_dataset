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
logfile=None

def checkEntity(sparql,link,name):
    global mid_found
    global mid_not_found
    global logfile
    fbmid=""
    link2 = "http://en.wikipedia.org/wiki/"+link
    query= ('''prefix : <http://rdf.freebase.com/ns/>
            select distinct ?entity {
            ?entity <http://rdf.freebase.com/ns/common.topic.topic_equivalent_webpage> <'''+link2+"> \n"
            '''} LIMIT 100''')
    sparql.setQuery(query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    try :
        results= sparql.query().convert()
    except SPARQLWrapper.SPARQLExceptions.QueryBadFormed:
        print("Bad Request",file=logfile)
        print (query,file=logfile)
        with open("badreqs.txt","a") as f:
            f.write(link2+"\n")
        return ([],"")
    if (len(results["results"]["bindings"])>=1):#should be a unique mid per page?
        result = results["results"]["bindings"][0]
        fbmid=result["entity"]["value"]
        print("mid found.",file=logfile)
        mid_found+=1
    else :
        print("no mids found.",file=logfile)
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
    print("checking for relations",file=logfile)
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



def process_file(wiki_file_path,sparql,tagger,sent_count,target_subdir,file):

    f=open(wiki_file_path,"r")
    print("processing "+wiki_file_path)
    logfile.write("processing "+wiki_file_path)
    sent_no=0
    sents=[]
    docurl=""
    docname=""
    for line in f:
        #end of an old doc
        if (line.startswith("</doc>")):
            continue
        #beginning of a new doc
        if(line.startswith("<doc")):
            soup=BeautifulSoup(line,"html.parser")
            docurl=soup.contents[0]["url"]
            docname=soup.contents[0]["title"]

        sent_no+=1

        sent_count+=1

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
                print("url1 : "+link1,file=logfile)
                chars=[]
                #replace spaces in the url with underscores
                #note that these are relative urls
                for character in link1 :
                    if character==" ":
                        character="_"
                    chars.append(character)
                link="".join(chars)
                print("url2 : " + link,file=logfile)
                name=content.string
                #add entity attributes
                entity["start"]=word_position
                entity["end"]=word_position+len(toks)
                entity["link"]=link
                entity["name"]=name
                (entity["labels"],entity["fbmid"])=checkEntity(sparql,link,name)
                entities.append(entity)
            word_position+=len(toks)

        if len(tokens)==0:
            continue
        tags = tagger.tag(tokens)
        tags2=[]
        for tag in tags:
            tags2.append(tag[1])
        sent["sentid"]=sent_no
        sent["tokens"]=tokens
        sent["tags"]=tags2
        sent["mentions"]=entities
        sent["relations"]=checkRelations(sparql,entities)
        sent["docname"]=docname
        sent["docurl"]=docurl
        sents.append(sent)
    
    if not os.path.exists(target_subdir):
        os.makedirs(target_subdir)
    with open(target_subdir+ "/"+file+".json","w") as jf:
        json.dump(sents,jf)
    f.close()
    return sent_count

''' Takes path to directory with folders and files extracted using WikiExtractor
    and path to output directory as inputs. Writes the sentences with required extracted information
    as Json files in output directory. 
    The Wikipedia dumps are extracted using the following command (preserve links)-

    python WikiExtractor.py -l enwiki-20160920-pages-articles-multistream.xml

'''
def create(wiki_directory,target_directory,subdir_start,subdir_end,process_no):
    global no_sents
    global logfile
    subdir_count=0
    sent_count=0

    logfile=open("../log"+str(process_no)+".txt","a")

    if not os.path.exists(target_directory):
        os.makedirs(target_directory)

    tagger = PerceptronTagger() 
    sparql = SPARQLWrapper.SPARQLWrapper("http://localhost:8890/sparql/")

    print("processing "+wiki_directory)
    logfile.write("processing "+wiki_directory+"\n")

    for subdir in os.listdir(wiki_directory):
        wiki_sub_directory=wiki_directory+"/"+subdir
        if not os.path.isdir(wiki_sub_directory):
            continue
        #process only subdirs from subdir_start to subdir_end
        if(subdir_count < subdir_start):
            subdir_count+=1
            print("skipping "+wiki_sub_directory)
            logfile.write("skipping "+wiki_sub_directory)
            continue
        if subdir_count >= subdir_end :
            break

        print("processing "+wiki_sub_directory)
        logfile.write("processing "+wiki_sub_directory+"\n")
        for file in os.listdir(wiki_directory+"/"+subdir):
            wiki_file_path=wiki_sub_directory+"/"+file
            target_subdir=target_directory+"/"+subdir
            sent_count=process_file(wiki_file_path,sparql,tagger,sent_count,target_subdir,file)
            file_count+=1
            #stop after single file
            # return
    no_sents=sent_count
    logfile.close()


if __name__ == "__main__":

    target_directory=sys.argv[2]
    wiki_directory = sys.argv[1]
    subdir_start=int(sys.argv[3])
    subdir_end=int(sys.argv[4])
    process_no=int(sys.argv[5])
    create(wiki_directory,target_directory,subdir_start,subdir_end,process_no)
    with open("stats.txt","w") as f :
        f.write("No of Sentences :"+str(no_sents))
        f.write("No of entities linked to freebase :"+str(mid_found))
        f.write("No of entities not found in freebase :"+str(mid_not_found))
        f.write("No of relations :"+str(no_relations))
