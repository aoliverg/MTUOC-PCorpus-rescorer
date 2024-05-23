# MTUOC-PCorpus-rescorer
A set of programs to rescore parallel corpora developed in the framework of the project **TAN-IBE: Neural Machine Translation for the romance languages of the Iberian Peninsula**, founded by the Spanish Ministry of Science and Innovation Proyectos de generación de conocimiento 2021. Reference: PID2021-124663OB-I00 founded by MCIN /AEI /10.13039/501100011033 / FEDER, UE.

## Prerequisites

By default, the program uses the fasttext language identification model [lid.176.bin](https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin). You should download this model to the same directory of the programs. You may use any other fastext language identification model and even train your own model, as explained below.

The file requirements.txt have the requirements to install in order to run the programs.

## MTUOC-PCorpus-rescorer.py

This program creates a SQLite database storing the following information:

* source: the source segment
* target: the target segment
* score: for those corpora with a confidence score, as CCMatrix, for example, this confidence score is stored in this field.
* L1: the source language code detected by fasttext
* confL1: the language detection confidence for source language
* L2: the target language code detected by fasttext
* confL2: the language detection confidence for target language
* cosine_score: the cosine similarity between source and target segments.

We can get the help of the program with option -h:

```
python3 MTUOC-PCorpus-rescorer.py -h
usage: MTUOC-PCorpus-rescorer.py [-h] -i INPUT -d DATABASE [-SEmodel SEMODEL] [-LDmodel LDMODEL]

MTUOC-PCorpus-rescorer: a script to score parallel corpora. The parallel corpus file should be a TSV file with source
segment, target segment and, optionally, a score. It creates a Sqlite database that should be used with the companion
program MTUOC-PCorpus-selector.

options:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        The input parallel corpus file.
  -d DATABASE, --database DATABASE
                        The SQLITE database file.
  -SEmodel SEMODEL      The SentenceTransformer model. Default model: LaBSE
  -LDmodel LDMODEL      The fasttext language detection model. Default model: lid.176.bin
```

Remember that you need a fasttext language model detection. You can get one writing:

`wget https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin`

To rescore the corpus corpus-eng-spa.txt we can write:

`python3 MTUOC-PCorpus-rescorer.py -i corpus-eng-spa.txt -d corpus-eng-spa.sqlite`



Once the rescoring process is finished we can us sqlite3 to access and process the data in the database. But a companion program, MTUOC-PCorpus-selector.py, will help us to do these actions.

## MTUOC-PCorpus-selector.py

This program also has the -h option that writes the help:

```
python3 MTUOC-PCorpus-selector.py -h
usage: MTUOC-PCorpus-selector.py [-h] -d DATABASE --sl SL --sldc SLDC --tl TL --tldc TLDC [-m MINSBERT] [-l LIMIT] -o
                                 OUTFILE

MTUOC-PCorpus-selector: a script to select parallel segments from a rescorer database created with MTUOC_PCorpus-
rescorer.

options:
  -h, --help            show this help message and exit
  -d DATABASE, --database DATABASE
                        The SQLITE database file.
  --sl SL               The source language code.
  --sldc SLDC           The minimum source language detection confidence.
  --tl TL               The target language code.
  --tldc TLDC           The minimum target language detection confidence.
  -m MINSBERT, --minSBERT MINSBERT
                        The minimum value for SBERT score.
  -l LIMIT, --limit LIMIT
                        The number of segments to be selected.
  -o OUTFILE, --outfile OUTFILE
                        The output file containing the parallel corpus.
```

If we want to select the segments with English as a source language with a confidence of 0.75, Spanish as a target language with a confidence of 0.75 and both segments being the translation equivalents with a confidence of 0.75, we can write:

`python3 MTUOC-PCorpus-selector.py -d corpus-eng-spa.sqlite --sl en --sldc 0.75 --tl es --tldc 0.75 -m 0.75 -o corpus-rescored.txt`

In the output file corpus-rescored.txt we will have the segments matching this condition.
