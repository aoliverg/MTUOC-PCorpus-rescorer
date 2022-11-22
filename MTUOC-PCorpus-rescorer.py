#    MTUOC-PCorpus-rescorer
#    Copyright (C) 2022  Antoni Oliver
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.

#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.

import sys
import os
import codecs
import sqlite3
import argparse
import fasttext
from sentence_transformers import SentenceTransformer, util


parser = argparse.ArgumentParser(description='MTUOC-PCorpus-rescorer: a script to score parallel corpora. The parallel corpus file should be a TSV file with source segment, target segment and, optionally, a score. It creates a Sqlite database that should be used with the companion program MTUOC-PCorpus-selector.')
parser.add_argument("-i","--input", type=str, help="The input parallel corpus file.", required=True)
parser.add_argument("-d","--database", type=str, help="The SQLITE database file.", required=True)
parser.add_argument("-s","--sl", type=str, help="The source language code language", required=True)
parser.add_argument("-t","--tl", type=str, help="The target language code language", required=True)
parser.add_argument("-SEmodel",type=str, help="The SentenceTransformer model. Default model: LaBSE", required=False, default="LaBSE")
parser.add_argument("-LDmodel",type=str, help="The fasttext language detection model. Default model: lid.176.bin", required=False, default="lid.176.bin")

args = parser.parse_args()

fentrada=args.input
database=args.database
l1=args.sl
l2=args.tl
SEmodel=args.SEmodel
LDmodel=args.LDmodel

#Available languages lid.176.bin fasttext model: af als am an ar arz as ast av az azb ba bar bcl be bg bh bn bo bpy br bs bxr ca cbk ce ceb ckb co cs cv cy da de diq dsb dty dv el eml en eo es et eu fa fi fr frr fy ga gd gl gn gom gu gv he hi hif hr hsb ht hu hy ia id ie ilo io is it ja jbo jv ka kk km kn ko krc ku kv kw ky la lb lez li lmo lo lrc lt lv mai mg mhr min mk ml mn mr mrj ms mt mwl my myv mzn nah nap nds ne new nl nn no oc or os pa pam pfl pl pms pnb ps pt qu rm ro ru rue sa sah sc scn sco sd sh si sk sl so sq sr su sv sw ta te tg th tk tl tr tt tyv ug uk ur uz vec vep vi vls vo wa war wuu xal xmf yi yo yue zh


modelFT = fasttext.load_model(LDmodel)
model = SentenceTransformer(SEmodel)


if os.path.isfile(database):
    os.remove(database)

conn=sqlite3.connect(database)
cur = conn.cursor() 
cur.execute("CREATE TABLE PCorpus(id INTEGER PRIMARY KEY, source TEXT, target TEXT, score FLOAT, detSL TEXT, SLconf FLOAT, detTL TEXT, TLconf FLOAT, scoreSBERT FLOAT)")
conn.commit()
entrada=codecs.open(fentrada,"r",encoding="utf-8")

cont=0
sources=[]
targets=[]

data=[]
for linia in entrada:
    linia=linia.rstrip()
    try:
        record=[]
        cont+=1        
        camps=linia.split("\t")
        source=camps[0]
        target=camps[1]
        if len(camps)>=3:
            score=float(camps[2])
        else:
            score=None
        DL1=modelFT.predict(source, k=1)
        DL2=modelFT.predict(target, k=1)
        L1=DL1[0][0].replace("__label__","")
        confL1=DL1[1][0]
        L2=DL2[0][0].replace("__label__","")
        confL2=DL2[1][0]
        if L1==l1 and L2==l2:
            embeddings1 = model.encode([source], convert_to_tensor=False)
            embeddings2 = model.encode([target], convert_to_tensor=False)
            #Compute cosine-similarities
            cosine_scores = util.cos_sim(embeddings1, embeddings2)
            cosine_score=cosine_scores[0][0].item()
        else:
            cosine_score=0
        record.append(source)
        record.append(target)
        record.append(score)
        record.append(L1)
        record.append(confL1)
        record.append(L2)
        record.append(confL2)
        record.append(cosine_score)
        data.append(record)
        if cont%1000==0:
            print(cont/1000,"K segments")
            cur.executemany("INSERT INTO PCorpus (source, target, score, detSL, SLconf, detTL, TLconf, scoreSBERT) VALUES (?,?,?,?,?,?,?,?)",data)
            data=[]
            conn.commit()
    except:
        print("ERROR:",sys.exc_info())
        
cur.executemany("INSERT INTO PCorpus (source, target, score, detSL, SLconf, detTL, TLconf, scoreSBERT) VALUES (?,?,?,?,?,?,?,?)",data)
data=[]
conn.commit()

