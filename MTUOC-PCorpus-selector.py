#    MTUOC-PCorpus-selector
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

import codecs
import sqlite3
import sys
import argparse

parser = argparse.ArgumentParser(description='MTUOC-PCorpus-selector: a script to select parallel segments from a rescorer database created with MTUOC_PCorpus-rescorer. ')
parser.add_argument("-d","--database", type=str, help="The SQLITE database file.", required=True)
parser.add_argument("--sldc", type=float, help="The minimum source language detection confidence.", required=True)
parser.add_argument("--tldc", type=float, help="The minimum target language detection confidence.", required=True)
parser.add_argument("-l","--limit", type=str, help="The number of segments to be selected.", required=True)
parser.add_argument("-o","--outfile", type=str, help="The output file containing the parallel corpus.", required=True)

args = parser.parse_args()
database=args.database
sldc=float(args.sldc)
tldc=float(args.tldc)
limit=args.limit
outfile=args.outfile

sortida=codecs.open(outfile,"w",encoding="utf-8")

conn=sqlite3.connect(database)
cur = conn.cursor() 
cur.execute("SELECT source,target, scoreSBERT FROM PCorpus where SLconf>="+str(sldc)+" and TLconf>="+str(tldc)+" ORDER BY scoreSBERT DESC limit "+str(limit)+";")

data=cur.fetchall()

for d in data:
    source=d[0]
    target=d[1]
    cadena=source+"\t"+target
    print(cadena)
    sortida.write(cadena+"\n")
