#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 22 16:13:19 2018

@author: andy
"""
import os
import pandas as pd
from datetime import datetime
import re

from elasticsearch import Elasticsearch, helpers
import whoosh.index as index
from whoosh.index import create_in
from whoosh.fields import *
from whoosh.analysis import StemmingAnalyzer

data_dir = "/Users/andy/data/hillary-clinton-emails"
csvs = [pd.read_csv(os.path.join(data_dir, csv), dtype=str).fillna("").to_dict(orient='records') for csv in os.listdir(data_dir) if os.path.splitext(csv)[1]=='.csv']
types = ['email_receivers', 'emails', 'aliases', 'persons']
data = dict(zip(types, csvs))

for email in data['emails']:
    try:
        email['DateSentFormatted'] = datetime.strptime(email["MetadataDateSent"], "%Y-%m-%dT%H:%M:%S+00:00")
    except ValueError:
        email["DateSentFormatted"] = datetime(1900, 1, 1)
        email["MetadataDateSent"] = datetime.strftime(datetime(1900, 1, 1), "%Y-%m-%dT%H:%M:%S+00:00" )
#Emails 
for key, dlist in data.items():
    for item in dlist:
        item.update({"_index":"hillary_emails", "_id":key+str(item["Id"]), "_type":key})

emails = data['emails']
#es = Elasticsearch()
#es.indices.create(index='hillary_emails', ignore=400)
#for key, dat in data.items():
#    helpers.bulk(es, dat)

'''
#Below is the process to index the emails

#just raw emails

##whoosh
##create schema first time index is initialized
schema = Schema(RawText=TEXT(analyzer=StemmingAnalyzer(), stored=True),
                DateSentFormatted=DATETIME(stored=True),
                DocNumber=KEYWORD(stored=False),
                MetadataSubject=TEXT(stored=True),
                MetadataTo=KEYWORD(stored=True),
                MetadataFrom=KEYWORD(stored=True),
                SenderPersonId=KEYWORD(stored=True),
                MetadataDateReleased=TEXT(stored=True),
                ExtractedBodyText=TEXT(analyzer=StemmingAnalyzer(), stored=True),
                ExtractedTo=KEYWORD(stored=True),
                ExtractedFrom=KEYWORD(stored=True),
                ExtractedSubject=TEXT(analyzer=StemmingAnalyzer(), stored=True)                
                )
##initiate index
indexdir = "//Users/andy/data/emails/whooshIndex"
if not os.path.exists(indexdir):
    os.mkdir(indexdir)
ix = index.create_in(indexdir, schema, indexname="emails")

ix = index.open_dir(indexdir, indexname="emails")

#initiate writer from index object
writer = ix.writer(limitmb=1024)

for email in emails:
    writer.add_document(RawText=email["RawText"],
                DateSentFormatted=email["DateSentFormatted"],
                DocNumber=email['DocNumber'],
                MetadataSubject=email["MetadataSubject"],
                MetadataTo=email["MetadataTo"],
                MetadataFrom=email["MetadataFrom"],
                SenderPersonId=email["SenderPersonId"],
                MetadataDateReleased=email['MetadataDateReleased'],
                ExtractedBodyText=email["ExtractedBodyText"],
                ExtractedTo=email['ExtractedTo'],
                ExtractedFrom=email["ExtractedFrom"],
                ExtractedSubject=email['ExtractedSubject'])
writer.commit()
'''
        
indexdir = "//Users/andy/data/emails/whooshIndex"
ix = index.open_dir(indexdir, indexname="emails")
#get field values
searcher = ix.searcher()
list(searcher.lexicon("ExtractedTo"))

#query setup
from whoosh.qparser import QueryParser
from whoosh.query import Term
from whoosh.query import Wildcard 

qp = QueryParser("RawText", schema=ix.schema)
q = qp.parse("Kenya")
#searcher = ix.searcher()


#use with statment to auto close
with ix.searcher() as s:
#    results = s.search(q)
    results = s.search(q, terms=True) #tells you which terms match
   
    # Was this results object created with terms=True?
if results.has_matched_terms():
    # What terms matched in the results?
    print(results.matched_terms())

    # What terms matched in each hit?
    for hit in results:
        print(hit.matched_terms())
##limit number of returns (default=10)
#results = s.search(q, limit=20)
##return results for page five with length of 20 per page
#results = s.search_page(q, 5, pagelen=20) 

#number of documents
numDocs = searcher.doc_count_all()

#sometimes convenient not to automatically close it using with statement
results = ix.searcher().search(q, terms=True)
for r in results:
    print(r)
ix.searcher.close()        
        