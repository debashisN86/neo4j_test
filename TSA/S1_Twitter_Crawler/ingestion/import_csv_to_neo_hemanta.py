import csv
import os
import datetime
import time
import sys
reload(sys)

sys.setdefaultencoding('utf-8')

from py2neo import Graph, Node, Relationship

# import unicodedata
# import itertools
# from itertools import product

graph = Graph('http://localhost:7474/db/data', user='neo4j', password='sparkserver')


graph.run("CREATE CONSTRAINT ON (m:MESSAGE) ASSERT m.TweetID IS UNIQUE;")
graph.run("CREATE CONSTRAINT ON (u:USER) ASSERT u.UserId IS UNIQUE;")


#graph.run("CREATE CONSTRAINT ON (h:HASHTAG) ASSERT h.hashtag_name IS UNIQUE;")



#graph.run("CREATE CONSTRAINT ON ()-[l:POST]-() ASSERT exists(l.tweet_id);")
#graph.run("CREATE CONSTRAINT ON ()-[t:TAGS]-() ASSERT exists(t.tweet_id);")
#graph.run("CREATE CONSTRAINT ON ()-[um:MENTIONS]-() ASSERT exists(um.author_id)")

#path = "neo4j/import/tawang/neo4j_csv/"

path = "/Users/debashisnaskar/Neo4j_Server/neo4j-community-3.5.5/import/neo4j_csv/"
dirs = os.listdir(path)
dirs.sort()
print(dirs)


def main():



    for folder in dirs:
    	if '.DS_Store' in folder:
    		continue
    	if not (folder.endswith('_t') or folder.endswith('_h')):
    		continue
    	print(folder, path, path+folder)
        files = os.listdir(path+folder)
        files.sort()
        print files





        for l in files:
            # print l
            fp = path+folder+"/"+l
            with open(fp,'r') as input_file:
                reader = csv.reader(input_file, delimiter=',')



                # print fp
                if l == "1_tweet_node.csv":
                    print fp
                    #fp = "file:///tawang/neo4j_csv/"+folder+"/"+l

                    fp = "file:///neo4j_csv/"+folder+"/"+l
                    print fp
                    #graph.run("CREATE CONSTRAINT ON (t:TWEET) ASSERT t.tweet_id IS UNIQUE")
                    #graph.run("CREATE CONSTRAINT ON (u:USER) ASSERT u.author_screen_name IS UNIQUE")

                    query = '''
                    USING PERIODIC COMMIT 10000
                    LOAD CSV WITH HEADERS FROM \''''+fp+'''\' AS row
                    
                    
                        
                    MERGE (t:MESSAGE {TweetID:row.TweetID})
                    on create set t.Date = row.Date
                    on create set t.TimeStamp = row.TimeStamp
                    on create set t.Content = row.Content
                    on create set t.Type = row.Type
                    on create set t.Tokenize = row.Tokenize
                    on create set t.Valence = row.Valence
                    on create set t.Arousal = row.Arousal
                    on create set t.Sentiment = row.Sentiment
                    on create set t.Emotion = row.Emotion
                    on create set t.sec_nonsec_tag = row.sec_nonsec_tag
                    on create set t.retweet_source_id = row.retweet_source_id
                    on create set t.replyto_source_id = row.replyto_source_id
                    on create set t.quoted_source_id = row.quoted_source_id
                    on create set t.like_count = row.like_count
                    on create set t.quote_count = row.quote_count
                    on create set t.reply_count = row.reply_count
                    on create set t.retweet_count = row.retweet_count
                    on create set t.tweet_lang = row.tweet_lang
                    on create set t.tweet_location = row.tweet_location
                    on create set t.UserId = row.ID1
                    
                    MERGE (u:USER {UserId:row.ID1})
                    on create set u.TweetID = row.TweetID
                    on create set u.ScreenName = row.ScreenName
                    on create set u.Date = row.Date
                    on create set u.UserName = row.UserName
                    on create set u.LMocation = row.LMocation
                    on create set u.Description = row.Description
                    on create set u.followers_count = row.followers_count
                    on create set u.listed_count = row.listed_count
                    on create set u.statuses_count = row.statuses_count
                    on create set u.friends_count = row.friends_count
                    on create set u.favourites_count = row.favourites_count

                    
                    
                    MERGE (u)-[:Sender]->(t)
                        
                    '''
                    graph.run(query)



                elif l == "2_qtweet_node.csv":
                    print fp
                    #fp = "file:///tawang/neo4j_csv/"+folder+"/"+l

                    fp = "file:///neo4j_csv/"+folder+"/"+l
                    print fp

                    query = '''
                    USING PERIODIC COMMIT 10000
                    
                    LOAD CSV WITH HEADERS FROM \''''+fp+'''\' AS row

                    MERGE (t:MESSAGE {TweetID:row.TweetID})
                    on create set t.Date = row.Date
                    on create set t.TimeStamp = row.TimeStamp
                    on create set t.Content = row.Content
                    on create set t.Type = row.Type
                    on create set t.Tokenize = row.Tokenize
                    on create set t.Valence = row.Valence
                    on create set t.Arousal = row.Arousal
                    on create set t.Sentiment = row.Sentiment
                    on create set t.Emotion = row.Emotion
                    on create set t.sec_nonsec_tag = row.sec_nonsec_tag
                    on create set t.retweet_source_id = row.retweet_source_id
                    on create set t.replyto_source_id = row.replyto_source_id
                    on create set t.quoted_source_id = row.quoted_source_id
                    on create set t.like_count = row.like_count
                    on create set t.quote_count = row.quote_count
                    on create set t.reply_count = row.reply_count
                    on create set t.retweet_count = row.retweet_count
                    on create set t.tweet_lang = row.tweet_lang
                    on create set t.tweet_location = row.tweet_location
                    on create set t.UserId = row.ID1
                    
                    MERGE (u:USER {UserId:row.ID1})
                    on create set u.TweetID = row.TweetID
                    on create set u.ScreenName = row.ScreenName
                    on create set u.Date = row.Date
                    on create set u.UserName = row.UserName
                    on create set u.LMocation = row.LMocation
                    on create set u.Description = row.Description
                    on create set u.followers_count = row.followers_count
                    on create set u.listed_count = row.listed_count
                    on create set u.statuses_count = row.statuses_count
                    on create set u.friends_count = row.friends_count
                    on create set u.favourites_count = row.favourites_count
                    
                    MERGE (u)-[:Sender]->(t)
                    
                    MERGE (k:USER {UserId:row.quoted_source_user_id})
                    on create set k.TweetID = row.quoted_source_id
                    
                    MERGE (t)-[:Receiver]->(k)
                    MERGE (m:MESSAGE {TweetID:row.quoted_source_id})
                    on create set m.UserId = row.quoted_source_user_id
                    
                    
                    MERGE (k)-[:Sender]->(m)
                    
                    '''
                    graph.run(query)

                elif l == "3_replyTweet_node.csv":
                    print fp
                    #fp = "file:///tawang/neo4j_csv/"+folder+"/"+l

                    fp = "file:///neo4j_csv/"+folder+"/"+l
                    print fp

                    query = '''
                    USING PERIODIC COMMIT 10000
                    
                    LOAD CSV WITH HEADERS FROM \''''+fp+'''\' AS row

                    MERGE (t:MESSAGE {TweetID:row.TweetID})
                    on create set t.Date = row.Date
                    on create set t.TimeStamp = row.TimeStamp
                    on create set t.Content = row.Content
                    on create set t.Type = row.Type
                    on create set t.Tokenize = row.Tokenize
                    on create set t.Valence = row.Valence
                    on create set t.Arousal = row.Arousal
                    on create set t.Sentiment = row.Sentiment
                    on create set t.Emotion = row.Emotion
                    on create set t.sec_nonsec_tag = row.sec_nonsec_tag
                    on create set t.retweet_source_id = row.retweet_source_id
                    on create set t.replyto_source_id = row.replyto_source_id
                    on create set t.quoted_source_id = row.quoted_source_id
                    on create set t.like_count = row.like_count
                    on create set t.quote_count = row.quote_count
                    on create set t.reply_count = row.reply_count
                    on create set t.retweet_count = row.retweet_count
                    on create set t.tweet_lang = row.tweet_lang
                    on create set t.tweet_location = row.tweet_location
                    on create set t.UserId = row.ID1
                    
                    MERGE (u:USER {UserId:row.ID1})
                    on create set u.TweetID = row.TweetID
                    on create set u.ScreenName = row.ScreenName
                    on create set u.Date = row.Date
                    on create set u.UserName = row.UserName
                    on create set u.LMocation = row.LMocation
                    on create set u.Description = row.Description
                    on create set u.followers_count = row.followers_count
                    on create set u.listed_count = row.listed_count
                    on create set u.statuses_count = row.statuses_count
                    on create set u.friends_count = row.friends_count
                    on create set u.favourites_count = row.favourites_count
                    
                    MERGE (u)-[:Sender]->(t)
                    
                    MERGE (k:USER {UserId:row.replyto_source_user_id})
                    on create set k.TweetID = row.replyto_source_id
                    
                    MERGE (t)-[:Receiver]->(k)	

                    MERGE (m:MESSAGE {TweetID:row.replyto_source_id})
                    on create set m.UserId = row.replyto_source_user_id
                    
                    
                    MERGE (k)-[:Sender]->(m)
                    '''
                    graph.run(query)


                elif l == "4_retweet_node.csv":
                    print fp
                    #fp = "file:///tawang/neo4j_csv/"+folder+"/"+l

                    fp = "file:///neo4j_csv/"+folder+"/"+l

                    print fp
                    #graph.run("CREATE CONSTRAINT ON (t:TWEET) ASSERT t.tweet_id IS UNIQUE")
                    #graph.run("CREATE CONSTRAINT ON (h:HASHTAG) ASSERT h.hashtag_name IS UNIQUE")


                    query = '''
                    USING PERIODIC COMMIT 10000
                    LOAD CSV WITH HEADERS FROM \''''+fp+'''\' AS row
                    
                    MERGE (t:MESSAGE {TweetID:row.TweetID})
                    on create set t.Date = row.Date
                    on create set t.TimeStamp = row.TimeStamp
                    on create set t.Content = row.Content
                    on create set t.Type = row.Type
                    on create set t.Tokenize = row.Tokenize
                    on create set t.Valence = row.Valence
                    on create set t.Arousal = row.Arousal
                    on create set t.Sentiment = row.Sentiment
                    on create set t.Emotion = row.Emotion
                    on create set t.sec_nonsec_tag = row.sec_nonsec_tag
                    on create set t.retweet_source_id = row.retweet_source_id
                    on create set t.replyto_source_id = row.replyto_source_id
                    on create set t.quoted_source_id = row.quoted_source_id
                    on create set t.like_count = row.like_count
                    on create set t.quote_count = row.quote_count
                    on create set t.reply_count = row.reply_count
                    on create set t.retweet_count = row.retweet_count
                    on create set t.tweet_lang = row.tweet_lang
                    on create set t.tweet_location = row.tweet_location
                    on create set t.UserId = row.ID1
                    
                    MERGE (u:USER {UserId:row.ID1})
                    on create set u.TweetID = row.TweetID
                    on create set u.ScreenName = row.ScreenName
                    on create set u.Date = row.Date
                    on create set u.UserName = row.UserName
                    on create set u.LMocation = row.LMocation
                    on create set u.Description = row.Description
                    on create set u.followers_count = row.followers_count
                    on create set u.listed_count = row.listed_count
                    on create set u.statuses_count = row.statuses_count
                    on create set u.friends_count = row.friends_count
                    on create set u.favourites_count = row.favourites_count
                    
                    MERGE (u)<-[:Receiver]-(t)
                    
                    MERGE (k:USER {UserId:row.retweet_source_user_id})
                    on create set k.TweetID = row.retweet_source_id
                    
                    MERGE (k)-[:Sender]->(t)
                    
                    
                    
                    
                    '''
                    graph.run(query)

                elif l == "5_mention_node.csv":
                    print fp
                    #fp = "file:///tawang/neo4j_csv/"+folder+"/"+l

                    fp = "file:///neo4j_csv/"+folder+"/"+l
                    print fp

                    query = '''
                    USING PERIODIC COMMIT 10000
                    
                    LOAD CSV WITH HEADERS FROM \''''+fp+'''\' AS row

                    MERGE (t:MESSAGE {TweetID:row.TweetID})
                    
                    MERGE (u:USER {UserId:row.ID1})
                    on create set u.TweetID = row.TweetID
                    on create set u.ScreenName = row.ScreenName
                    on create set u.UserName = row.UserName
                    
                    

                    MERGE (t)-[:Receiver]->(u)
                    '''
                    graph.run(query)




        home_dir = os.environ['HOME']
        #dir_name = home_dir+"/neo4j/import/tawang/neo4j_csv/"
        dir_name = home_dir + "/Neo4j_Server/neo4j-community-3.5.5/import/neo4j_csv/"
        f= open(dir_name+folder+".neo", "w")
        f.close()

                # elif l == "4_user_node.csv":
                # 	print fp
                # 	fp = "file:///neo4j_csv/"+folder+"/"+l
                # 	print fp

                # 	query = '''

                # 	LOAD CSV WITH HEADERS FROM \''''+fp+'''\' AS row
                # 	CREATE (u:USER)
                # 	SET u=row
                # 	'''
                # 	graph.run(query)


    # q1 = '''

    # MATCH (t:TWEET),(u:USER)
    # WHERE t.tid = u.tid
    # CREATE (u)-[:POST {datetime: t.datetime}]->(t)

    # '''

    # graph.run(q1)

    # q2 = '''
    # MATCH (h:HASHTAG),(t:TWEET)
    # WHERE h.tid = t.tid
    # CREATE (h)-[:TAGS]->(t)

    # '''
    # graph.run(q2)

    # q3 = '''
    # MATCH (u:USER),(t:TWEET)
    # WHERE u.mtid = t.tid
    # CREATE (t)-[:MENTIONS]->(u)

    # '''
    # graph.run(q3)




if __name__ == '__main__':
    start = time.time()
    print time.asctime( time.localtime(time.time()) )
    main()
    print time.asctime( time.localtime(time.time()) )
    end = time.time() - start
    print "Time to complete:", end



#MERGE (t:TWEET {tweet_id:row.tid})
#on create set t.tweet_text = row.text
#on create set t.datetime = row.datetime



