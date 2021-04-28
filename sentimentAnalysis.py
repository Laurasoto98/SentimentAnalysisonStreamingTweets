#!/usr/bin/env python
#!/bin/bash 
#!python -m textblob.download_corpora

import pyspark
import sys
import subprocess
import nltk

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

install("textblob")

from textblob import TextBlob

if len(sys.argv) != 3:
  raise Exception("Exactly 2 arguments are required: <inputUri> <outputUri>")

inputUri=sys.argv[1]
outputUri=sys.argv[2]

sc = pyspark.SparkContext()
rdd = sc.textFile(sys.argv[1])

def removeNonAlpabet(s):
    return ''.join([i.lower() for i in s if i.isalpha() or i==' ']).lstrip().rstrip()

nltk.download('words')
english_words = set(nltk.corpus.words.words())

def removeNonEnglish(s):
    return ''.join([i.lower() for i in s if i in english_words or i==' ']).lstrip().rstrip()

rdd = rdd.map(removeNonAlpabet).map(removeNonEnglish).filter(lambda x: x!='').distinct()

def polarity(t):
  return TextBlob(t).sentiment.polarity

def sentiment(x):
  if x < 0:
    return 'negative'
  if x==0.0:
    return 'neutral'
  else:
    return 'positive'

rdd= rdd.map(lambda x: (x, polarity(x))).map(lambda x: (sentiment(x[1]),1)).reduceByKey(lambda x,y:(x+y))
rdd.saveAsTextFile(sys.argv[2])