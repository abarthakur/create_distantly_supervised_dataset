import nltk
from nltk.tag.perceptron import PerceptronTagger
# import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup,Tag
import urllib


def checkEntity(link,name):
    return []

def checkRelations(entities):
    return []

tagger = PerceptronTagger() 

f= open ("./data_temp/text/AA/wiki_00")
docline=f.readline()
soup=BeautifulSoup(docline,"html.parser")
docurl=soup.contents[0]["url"]
docname=soup.contents[0]["title"]
x=3
lineno=0
sents=[]
for line in f:
    # line="<line>"+line+"</line>"
    # root = ET.fromstring(line)
    # print(root)
    sent={}
    lineno+=1
    # print(lineno)
    n=0
    tokens=[]
    tags=[]
    entities=[]
    relations=[]
    soup = BeautifulSoup(line,"html.parser")
    for content in soup.contents:
        string=content.string
        tok= nltk.word_tokenize(string)
        tokens+=tok
        # print(n,content)
        if content.name =="a":
            entity={}
            link= urllib.parse.unquote(content["href"])
            name=content.string
            entity["start"]=n
            entity["end"]=n+len(tok)
            entity["link"]=link
            entity["name"]=name
            entity["labels"]=checkEntity(link,name)
            entities.append(entity)
        n+=len(tok)
    tags = tagger.tag(tokens)
    tags2=[]
    for tag in tags:
        tags2.append(tag[1])
    # print(tokens)
    # print(entities)
    sent["sentid"]=lineno
    sent["tokens"]=tokens
    sent["tags"]=tags2
    sent["mentions"]=entities
    sent["relations"]=checkRelations(entities)
    sent["docname"]=docname
    sent["docurl"]=docurl
    print(sent)
    # print(tags)
    x-=1
    if(x==0):
        break

