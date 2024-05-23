import codecs
import sys

corpusIN=sys.argv[1]
corpusOUT=sys.argv[2]
language=sys.argv[3]

entrada=codecs.open(corpusIN,"r",encoding="utf-8")
sortida=codecs.open(corpusOUT,"w",encoding="utf-8")

prefix="__prefix__"+language

for linia in entrada:
    linia=linia.rstrip()
    liniamod=prefix+" "+linia
    sortida.write(liniamod+"\n")
    
