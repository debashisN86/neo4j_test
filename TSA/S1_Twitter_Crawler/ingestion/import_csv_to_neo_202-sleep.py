import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

import csv, logging, signal, traceback, glob, json
import os
import datetime
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from py2neo import Graph, Node, Relationship
from py2neo.database import DatabaseError
from py2neo.database import TransientError


# import unicodedata
# import itertools
# from itertools import product

graph = Graph('http://localhost:7474/db/data', user='neo4j', password='sparkserver')


graph.run("CREATE CONSTRAINT ON (m:MESSAGE) ASSERT m.TweetID IS UNIQUE;")
graph.run("CREATE CONSTRAINT ON (u:USER) ASSERT u.UserId IS UNIQUE;")



path = "/Users/debashisnaskar/Neo4j_Server/neo4j-community-3.5.5/import/neo4j_csv/"

#path = "neo4j/import/neo4j_csv/"
dirs = os.listdir(path)
dirs.sort()



#Create and configure logger                                                   
logging.basicConfig(filename="import_csv_to_neo_202.log", format='INFO %(asctime)s %(message)s', filemode='a')
#Creating an object                                                            
logger=logging.getLogger()                                                     
#Setting the threshold of logger to DEBUG                                      
logger.setLevel(logging.INFO) 
#logger.info("5698999")
current_file = None

def get_traceback_info_of_exception(e, user_defined_err_no):
    exc_type, exc_value, exc_traceback = sys.exc_info()
    logger.info ("*** exc_traceback.tb_lineno:  " + str(exc_traceback.tb_lineno)) ## give the line no. where exception occur
    logger.info(str(user_defined_err_no)+" " + str(e))



def sigint_handler(signum, frame):
    global current_file
    logger.info('Stop pressing the CTRL+C!')
    if current_file:
        ## if 'ctrl+c' button pressed, delete the current (.start) file created by this process..............
        try:
            os.remove(current_file)
        except OSError, e:
            pass
        logger.info('My application connection broken1')
        sys.exit()



def exit_handler():
    global current_file
    if current_file:
        ## if unexpected kill occur, delete the current (.start) file created by this process..............
        try:
            os.remove(current_file)
        except OSError, e:
            pass
        logger.info('My application connection broken2')
        sys.exit()


def import_csv_instruction(file_name, dir_path_from_197, folder_name):
    fp = dir_path_from_197+folder_name+"/"+file_name
    logger.info(fp)
    try:
        with open(fp,'r') as input_file:
            reader = csv.reader(input_file, delimiter=',')
            # print fp
            if file_name == "1_tweet_node.csv":
                print fp

                fp = "file:///neo4j_csv/" + folder_name + "/" + file_name


                query = '''
                                USING PERIODIC COMMIT 10000
                                LOAD CSV WITH HEADERS FROM \'''' + fp + '''\' AS row



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



            elif file_name == "2_qtweet_node.csv":
                print fp

                fp = "file:///neo4j_csv/" + folder_name + "/" + file_name

                query = '''
                                USING PERIODIC COMMIT 10000

                                LOAD CSV WITH HEADERS FROM \'''' + fp + '''\' AS row

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

            elif file_name == "3_replyTweet_node.csv":
                print fp

                fp = "file:///neo4j_csv/" + folder_name + "/" + file_name

                query = '''
                                USING PERIODIC COMMIT 10000

                                LOAD CSV WITH HEADERS FROM \'''' + fp + '''\' AS row

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


            elif file_name == "4_retweet_node.csv":
                print fp

                fp = "file:///neo4j_csv/" + folder_name + "/" + file_name



                query = '''
                                USING PERIODIC COMMIT 10000
                                LOAD CSV WITH HEADERS FROM \'''' + fp + '''\' AS row

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

            elif file_name == "5_mention_node.csv":
                print fp

                fp = "file:///neo4j_csv/" + folder_name + "/" + file_name

                query = '''
                                USING PERIODIC COMMIT 10000

                                LOAD CSV WITH HEADERS FROM \'''' + fp + '''\' AS row

                                MERGE (t:MESSAGE {TweetID:row.TweetID})

                                MERGE (u:USER {UserId:row.ID1})
                                on create set u.TweetID = row.TweetID
                                on create set u.ScreenName = row.ScreenName
                                on create set u.UserName = row.UserName



                                MERGE (t)-[:Receiver]->(u)
                                '''
                graph.run(query)

        return "success123"
    except DatabaseError, e:
        logger.info("DataBaseError ------------------>")
        logger.info("name of the exception(e)--------------> "+str(type(e).__name__))
        logger.info("message 0f e---------------> "+str(e))
        logger.info("<--------------------------------------------->")
        return "DataBaseError"
    except Exception, e:
        logger.info("file error ------------------>")
        logger.info("name of the exception(e)---------------> "+str(type(e).__name__))
        logger.info("message 0f e---------------> "+str(e))
        ## import_csv_to_neo4j(folder_name, dir_path_from_197)
        #return "write_lock_error"
        sys.exit()



def import_csv_to_neo4j(folder_name, dir_path_from_197):	
    logger.info("again started")
    try:
        dir_path_from_197 = dir_path_from_197+"/"
        # for folder_name in dir_path_from_197:
        if len(os.listdir(dir_path_from_197+folder_name)) == 4:
            files = os.listdir(dir_path_from_197+folder_name)
            files.sort()
            for file1 in files:
                logger.info("file name --> "+file1)
                try:
                    res = import_csv_instruction(file1, dir_path_from_197, folder_name)
                    if res == "DataBaseError":
                        ## do not delete the (.neostart) file.....................................
                        #try:
                            #os.remove(dir_path_from_197+folder_name+".neostart")
                        #except OSError, e:
                            #pass
                        return
                except Exception, e:
                        logger.info("Warning: File is not successfully created yet. After creating successfully, csv file generation will start...")
                        logger.info("message 0f e---------------> "+str(e))
                        # import_csv_to_neo4j(folder_name, dir_path_from_197)
                        sys.exit()
        else:
            import_csv_to_neo4j(folder_name, dir_path_from_197)
        f=open(dir_path_from_197+folder_name+".neo", "w")
        f.close()
        logger.info("completed")
        ## delete the (.neostart) file.....................................
        try:
            os.remove(dir_path_from_197+folder_name+".neostart")
        except OSError, e:
            pass
    except ValueError, e:
        user_defined_err_no = "2rt"
        get_traceback_info_of_exception(e, user_defined_err_no)
        logger.info("pass")
        pass



def run(neo4j_path, dir_path_from_197):
    global current_file
    # logger.info("check")
    #list_of_files_with_json = list()
    list_of_files_with_neo = list()
    list_of_files_with_neostart = list()

    temp_l = os.listdir(dir_path_from_197)
    temp_l.reverse()
    ## take all .neo file name and store in a list
    for folder in temp_l:
        if folder.endswith(".neo"):
            list_of_files_with_neo.append(folder.split(".")[0])
            # print "neo:  ", list_of_files_with_neo
    ## take all .neostart file name and store in a list
    for folder in temp_l:
        if folder.endswith(".neostart"):
            list_of_files_with_neostart.append(folder.split(".")[0])
            # print "neostart:  ", list_of_files_with_neostart
    ## take all folder which does not have a .neo file name and thn store in a list
    for folder in temp_l:
        if not (folder.endswith(".neo") or folder.endswith(".neostart")):
            # fn = folder.split(".")[0]
            if folder not in list_of_files_with_neo and folder not in list_of_files_with_neostart:
                # list_of_files_with_json.append(fn)
                try:
                    ## take the folder for insertion ...........................(.neostart)........empty file
                    f=os.open(dir_path_from_197+"/"+folder+".neostart", os.O_CREAT | os.O_EXCL)
                    current_file = dir_path_from_197+"/"+folder+".neostart"
                    os.close(f)
                except OSError, e:
                    ## whatever (.json) file choosen, if (.neostart) file for that (.json) file is present then abort this function to process further
                    if e.strerror == "File exists":
                        break
                t1 = time.time()
                logger.info("started for ----------------------------------------------> "+str(folder))
                import_csv_to_neo4j(folder, dir_path_from_197)
                logger.info("completed for ----------------------------------------------> "+str(folder))
                logger.info("Time for completion ----> "+str(time.time() - t1))
                break


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, sigint_handler)
    signal.signal(signal.SIGINT, sigint_handler)
    #signal.signal(signal.SIGKILL, sigint_handler)

    # os.environ['DIR'] = '/var/opt/'
    neo4j_path = "/Users/debashisnaskar/Neo4j_Server/neo4j-community-3.5.5/import/neo4j_csv"
    # home_dir = os.environ['DIR']
    # dir_path_from_197 = home_dir+"/"+neo4j_path
    dir_path_from_197 = '/Users/debashisnaskar/Neo4j_Server/neo4j-community-3.5.5/import/neo4j_csv'
    while(1):
        run(neo4j_path, dir_path_from_197)
        # check if we need to stop script
        if( os.path.isfile("__stop_neo4j_import__") ):
            logger.info(">>> Found stop file...stop <<<")
            break
        # sleep a little bit -- avoid busy-wait
        time.sleep(2)

    # run clean up code on ctrl-c
    # atexit.register(exit_handler)

