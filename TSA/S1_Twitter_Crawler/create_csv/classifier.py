# -*- coding: utf-8 -*-
from __future__ import division
import operator
from nltk.corpus import stopwords
import random
import re
import pickle
import gc
import json
from sklearn.naive_bayes import MultinomialNB


# security - 1
# non security - 0

stop = set(stopwords.words('english')).union({'url','rt'})


def classify_twitter_test(arg):
	#print "Testing..."
	tweet_string = arg
	filename = 'finalized_model.sav'
	fl = open(filename, 'rb')
	loaded_model = pickle.load(fl)
	fe = open('features.json')
	features = json.load(fe)
	#topic = #EXTRACT_TOPIC
	content = re.sub('[^a-z ]', '',tweet_string.lower())
	content = set(content.strip().split()) - stop
	temp_dict = dict()
	for k in content:
		if k.startswith('http') or k.startswith('https') or k.startswith('www'):
			continue
		temp_dict[k] = 1
	#print temp_dict	
	temp_list = []
	for f in features:
		if f in temp_dict:
			temp_list.append(1)
		else:
			temp_list.append(0)
	test_X_vector = list()
	test_X_vector.append(temp_list)
	result = loaded_model.predict(test_X_vector)
	fl.close()
	fe.close()
	return result

def get_tag_security_nonsec(arg):
#if __name__ == '__main__':
	classify_twitter_test(arg)
