# -*- coding: utf-8 -*-
import sys
import os
reload(sys)
sys.setdefaultencoding('utf-8')
import datetime

from datetime import timedelta
import pickle
import time
import gc
import json
import nltk
from nltk import word_tokenize
from nltk.util import ngrams
from nltk.collocations import *
import pprint
import re
from collections import OrderedDict
from operator import itemgetter
from pattern.en import parse
import string






### START Keyword extraction ### 

def extract_link(text):
	regex = r'https?://[^\s<>"]+|www\.[^\s<>)"]+'
	match = re.findall(regex, text)
	links = [] 
	for x in match: 
		if x[-1] in string.punctuation: links.append(x[:-1])
		else: links.append(x)
	return links
	
def cleanup(query): 
	try:
		urls = extract_link(" " + query + " ")
		for url in urls: 
			query = re.sub(url, "", query)
		q = query.strip()
	except:
		q = query
	q = re.sub(' RT ', '', ' ' + q + ' ').strip() 
	return q 


def convert_tag_format(query): 
	word = query.split(' ')
	postag = [(x.split('/')[0], x.split('/')[1]) for x in word]
	return postag 
	

def get_pos_tags(text): 
	tagged_sent = parse(text)	
	return convert_tag_format(tagged_sent), tagged_sent
	
def normalise(word):
	word = word.lower()
	return word


## conditions for acceptable word: length, stopword
def acceptable_word(word):
    accepted = bool(4 <= len(word) <= 40
        and word.lower() not in stopwords)
    return accepted

## extract entity from BIO encoding 
def extract_entity(filetext):
	last_entity = '' 
	last_tag = '' 
	mention2entities = {} 
	for line in filetext.split('\n'): 
		line = line.strip() 
		if line == '': 
			continue
		line_split = line.split('\t')
		if re.search('B-', line_split[1]): 
			if last_entity != '': 
				if not last_tag in mention2entities:
					mention2entities[last_tag] = [] 
				mention2entities[last_tag].append(last_entity.strip())
			last_entity = line_split[0] + ' '
			last_tag = line_split[1][2:] 
		elif re.search('I-', line_split[1]): 
			last_entity += line_split[0] + ' '
	if last_entity != '': 
		if not last_tag in mention2entities:
			mention2entities[last_tag] = [] 
		mention2entities[last_tag].append(last_entity.strip())
	return 	mention2entities

	
def get_entities_from_phrase(tagged_sent, phrase2consider): 
	word = tagged_sent.split(' ')
	bio_tags = [normalise(x.split('/')[0])+ '\t'+ x.split('/')[2] for x in word]
	bio_text = '\n'.join(bio_tags)
	mention2entities = extract_entity(bio_text)
	#print mention2entities.keys() 
	
	## strip off unacceptable words 
	_mention2entities = {} 
	for mention in mention2entities: 
		if not mention in phrase2consider: 
			continue
		_mention2entities[mention] = [] 
		for entity in mention2entities[mention]: 
			_entity = ' '.join([word for word in entity.split(' ') if acceptable_word(word)]).strip()
			if _entity != '': 
				_mention2entities[mention].append(_entity)
			
	entities = []
	for mention in _mention2entities: 
		entities.extend(_mention2entities[mention])
	return entities	
	

def getKeywords(text, phrase2consider=['NP', 'ADJP']): 
	_text = cleanup(text)
	try:
		postoks, tagged_sent = get_pos_tags(_text)
		entities = get_entities_from_phrase(tagged_sent, phrase2consider)
	except: 
		return []
	return entities

### END Keyword extraction ### 


symbols = ['&amp;', '!',',',';','.','(',')','\'']
stopwords = ['sir','day','title','shri','crore','day','time',"a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also","although","always","am","among", "amongst", "amoungst", "amount",  "an", "and", "another", "any","anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as",  "at", "back","be","became", "because","become","becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the"]






#find keyword from one text
def find_keyword(tweet_text):
	# print tweet_text
	x=tweet_text
	# print type(x)
	if x is not None:
		x=x.strip()	
		#print x
		#sys.exit()
		token = x.strip().split()
		l=[]
		for t in token :
			if t.find('@')>=0 or t.find('#') >=0 or t.find('…') >=0 or t.find('http:') >=0 or t.find('https:') >=0  :
				pass
			else :
				l.append(t)
		bi=[]			
		temp = ' '.join(l)			
		keywords_list = getKeywords(temp)
		# print '&&&&&&&&&'
		# print keywords_list



	return keywords_list
