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
#from pattern.en import parse
import operator
import string




def extract_link(text):
	regex = 'https?://[^\s<>"]+|www\.[^\s<>)"]+'
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




def get_sentiment(stopwords, positivewords, negativewords, tweet):
	pos_count = 0
	neg_count = 0
	sent = 0
	token = tweet.strip().split()
	for t in token :
		# print t
		if t in stopwords:
			pass
		elif t.find('…') >=0 or t.find('http:') >=0 or t.find('https:') >=0:
			pass
		else:
			if t in positivewords:
				# print "...found pos...."
				pos_count = pos_count + 1
			elif t in negativewords:
				# print "...found neg...."
				neg_count = neg_count + 1
			else:
				sent = 0


	if pos_count > neg_count:
		sent = 1
	elif pos_count < neg_count:
		sent = -1
	elif (pos_count == neg_count) and (pos_count > 0) and (neg_count > 0):
		sent = 1
	else:
		sent = 0

	# print pos_count, neg_count, sent
	return sent





# if __name__ == '__main__':

def to_get_sentiment_of_a_tweet(x):
	stopwords = ['sir','day','title','shri','crore','day','time',"a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also","although","always","am","among", "amongst", "amoungst", "amount",  "an", "and", "another", "any","anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as",  "at", "back","be","became", "because","become","becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the"]



	# to read the file and save into positive_lexicon_list
	fd = open('positivewords.txt','r')
	positivewords = []
	for line in fd :
		positivewords.append(line.strip())
	fd.close()
	# print positivewords

	

	# to read the file and save into -ve_lexicon_list
	fd = open('negativewords.txt','r')
	negativewords = []
	for line in fd :
		negativewords.append(line.strip())
	fd.close()
	# print negativewords




	# x = 'RT http is very #better  https://www.google.in.'
	x=x.strip()	
	r = cleanup(x)	
	senti = get_sentiment(stopwords, positivewords, negativewords, r)


	return senti
		
	
	

