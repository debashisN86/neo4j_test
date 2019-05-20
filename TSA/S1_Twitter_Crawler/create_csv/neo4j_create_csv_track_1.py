'''
created: 2018-10-22
Info : 
    get json file convert into csv
'''
import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")
import time, json, sys, datetime, traceback, pprint, logging, os, csv, glob, signal
from sentiment_module import to_get_sentiment_of_a_tweet
from find_keyword import find_keyword
from classifier import classify_twitter_test
reload(sys)
sys.setdefaultencoding('utf-8')
# import urllib2
# import unicodedata
# import requests
# import pickle

#Create and configure logger
logging.basicConfig(filename="neo4j_create_csv_track_1.log", format='INFO %(asctime)s %(message)s', filemode='a')
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



def get_data(twdata):
    global count

    mention_node_list = list()

    if not isinstance(twdata, dict):
        twdata=twdata.__dict__
    bundle_id1 = twdata['id_str']
    count += 1
    truncated = twdata['truncated']
    d1 = twdata['created_at']
    d2 = twdata['created_at']
    try:
        ##for date.......................
        if isinstance(d1, datetime.datetime):
            date = d1.date()
        else:
            d1 = datetime.datetime.strptime(d1, "%a %b %d %H:%M:%S +0000 %Y")
            #time = d1.time()

    except AttributeError, e:
        user_defined_err_no = 1
        get_traceback_info_of_exception(e, user_defined_err_no)

    if truncated:
        extended_tweet_dict = twdata['extended_tweet']
        tweet_text = extended_tweet_dict['full_text']
        entities = extended_tweet_dict['entities']
        ##for mentions...........
        if entities.has_key('user_mentions'):
            mentions_list = entities['user_mentions']  ## in list format from crawler
            mentions_list1 = []
            mentions_list_id = []
            mentions_name_list = []
            for m in mentions_list:
                sn = m['screen_name']
                mentions_list1.append(sn)
                mentions_list_id.append(m['id_str'])
                mentions_name_list.append(m['name'])
    else:
        tweet_text = twdata['text']
        ##for mentions...........
        if twdata['entities'].has_key('user_mentions'):
            mentions_list = twdata['entities']['user_mentions']
            mentions_list1 = []
            mentions_list_id = []
            mentions_name_list = []
            for m in mentions_list:
                sn = m['screen_name']
                mentions_list1.append(sn)
                mentions_list_id.append(m['id_str'])
                mentions_name_list.append(m['name'])

    is_quote_status = twdata['is_quote_status']
    ##if tweet is a retweet or quoted or reply or tweet........
    try:
        type1 = 'Tweet'
        quoted_source_id = None
        replyTo_source_id = None
        retweet_source_id = None
        retweet_source_user_id = None
        replyto_source_user_id = None
        quoted_source_user_id = None

        ##if tweet is a retweet of a TWEET........
        if twdata.has_key('retweeted_status'):
            if not isinstance(twdata['retweeted_status'], dict):
                twdata['retweeted_status']=twdata['retweeted_status'].__dict__
            try:
                type1 = 'Retweet'
                retweet_source_id = twdata['retweeted_status']['id_str']  #original tweet ID
                retweet_source_user_id = twdata['retweeted_status']['user']['id_str']
                quoted_source_id = None
                replyTo_source_id = None
                replyto_source_user_id = None
                quoted_source_user_id = None
            except Exception, e:
                user_defined_err_no = 90909090
                get_traceback_info_of_exception(e, user_defined_err_no)

        ##if tweet is a "RT of QT" and "QT"........
        if twdata['is_quote_status']:
            ##if tweet is a retweet of a QTWEET........
            if twdata.has_key('retweeted_status'):
                if not isinstance(twdata['retweeted_status'], dict):
                    twdata['retweeted_status']=twdata['retweeted_status'].__dict__
                try:
                    type1 = 'Retweet'
                    retweet_source_id = twdata['retweeted_status']['id_str']
                    retweet_source_user_id = twdata['retweeted_status']['user']['id_str']
                    quoted_source_id = None
                    replyTo_source_id = None
                    replyto_source_user_id = None
                    quoted_source_user_id = None
                except Exception, e:
                    user_defined_err_no = 234
                    get_traceback_info_of_exception(e, user_defined_err_no)
            else:
                try:
                    ##if tweet is a quoted........
                    type1 = 'QuotedTweet'
                    quoted_source_id = twdata['quoted_status_id_str']
                    quoted_source_user_id = twdata['quoted_status']['user']['id_str']
                    retweet_source_id = None
                    replyTo_source_id = None
                    retweet_source_user_id = None
                    replyto_source_user_id = None
                except Exception, e:
                    ## id 'is_quote_status' true but 'quoted_status_id_str' not present then treat that QT as a Tweet
                    type1 = 'Tweet'
                    quoted_source_id = None
                    replyTo_source_id = None
                    retweet_source_id = None
                    retweet_source_user_id = None
                    replyto_source_user_id = None
                    quoted_source_user_id = None

                    # print str(234234234)
                    user_defined_err_no = "sjh22"
                    get_traceback_info_of_exception(e, user_defined_err_no)
                    # sys.exit()

        ##if tweet is a reply........
        if twdata['in_reply_to_status_id_str'] is not None:
            #if twdata['in_reply_to_status_id_str'] is not None:
            type1 = 'Reply'
            if twdata.has_key('in_reply_to_status_id_str'):
                replyTo_source_id = twdata['in_reply_to_status_id_str']
                replyto_source_user_id = twdata['in_reply_to_user_id_str']
            quoted_source_id = None
            retweet_source_id = None
            retweet_source_user_id = None
            quoted_source_user_id = None

        #only tweet...........
        if not (twdata.has_key('retweeted_status') or twdata.has_key('quoted_status') or is_quote_status):
            type1 = 'Tweet'
            quoted_source_id = None
            replyTo_source_id = None
            retweet_source_id = None
            retweet_source_user_id = None
            replyto_source_user_id = None
            quoted_source_user_id = None
    except Exception, e:
        user_defined_err_no = 23344
        get_traceback_info_of_exception(e, user_defined_err_no)

    #for lang and keywords...............
    if twdata['lang'] is not None:
        tweet_lang = twdata['lang']
        ##for keyword...........
        if tweet_lang == 'en':
            keyword_list = find_keyword(tweet_text)
        else:
            keyword_list = []

    ## to identified Security related tweets
    secc_list = classify_twitter_test(tweet_text) #[0] or [1]
    if secc_list[0] == 0:
        secc = 's'
    elif secc_list[0] == 1:
        secc = 'ns'
    else:
        secc = ''

    #for author and geo and timezone............
    try:
        user_dict=twdata['user']
        if not isinstance(twdata['user'], dict):
            user_dict=twdata['user'].__dict__
        author_name = user_dict['name']
        author_screen_name = user_dict['screen_name']
        author_id = user_dict['id_str']
        author_location = user_dict['location']
        verified = str(user_dict['verified'])  #text
        Description = user_dict['description']
        if Description:
            Description = Description.strip().replace("\n", " ").replace("\"", " ").replace("'", " ")
        followers_count = user_dict['followers_count']
        listed_count = user_dict['listed_count']
        statuses_count = user_dict['statuses_count']
        friends_count = user_dict['friends_count']
        favourites_count = user_dict['favourites_count']
    except Exception, e:
        # logger.info(str(sys.exc_traceback.tb_lineno))
        user_defined_err_no = 5698999
        get_traceback_info_of_exception(e, user_defined_err_no)
        sys.exit()


    #for sentiment.................
    r = to_get_sentiment_of_a_tweet(tweet_text)
    sentiment = r

    #for emotion.....
    # em_list = give_jar_file_and_arg(tweet_text)
    # emotion = em_list[0]
    emotion = None
    Valence = None
    Arousal = None

    ##tweet_place..............
    tweet_location = None
    if twdata['place']:
        tweet_location = twdata['place']['name'] + ", " + twdata['place']['country']

    ## tweet_text..................................
    if tweet_text:
        tweet_text = tweet_text.strip().replace("\n", " ").replace("\"", " ").replace("'", " ")

    ## timestamp.................
    timestamp = time.mktime(d1.timetuple())  #d1.timestamp()

    ## tweet_type..............
    if type1 == 'Tweet':
        if len(mentions_list_id) > 0:
            type1 = 'T_Mention'
    elif type1 == 'Reply':
        if len(mentions_list_id) > 1:
            type1 = 'R_Mention'
    elif type1 == 'QuotedTweet':
        if len(mentions_list_id) > 0:
            type1 = 'QT_Mention'

    # tweet_node_list = list()
    # print twdata['id_str']
    tweet_node_list = [d1, timestamp, twdata['id_str'], tweet_text, type1, keyword_list, Valence, Arousal, sentiment, emotion, secc, retweet_source_id, retweet_source_user_id, replyTo_source_id, replyto_source_user_id, quoted_source_id, quoted_source_user_id, twdata['favorite_count'], twdata['quote_count'], twdata['reply_count'],  twdata['retweet_count'], tweet_lang, tweet_location, author_id, author_name, author_screen_name, author_location, Description, followers_count, listed_count, statuses_count, friends_count, favourites_count]

    if type1 == 'Retweet':
        mention_node_list = None
    else:
        m_count = 0
        for i1 in range(len(mentions_list_id)):
            if m_count == 0:
                if type1 == 'Reply' or type1 == 'R_Mention' or type1 == 'Retweet':
                    m_count += 1
                    continue
            mention_node_list.append([twdata['id_str'], mentions_list_id[i1], mentions_name_list[i1], mentions_list1[i1]])
            m_count += 1

    # print type1, twdata['id_str'], mention_node_list
    return [tweet_node_list, mention_node_list]








def run_for_retweet_quoted_status(twdata, tweet_node_list1, QTWEET_node_list2,  ReplyTweet_node_list3, Retweet_node_list4, mention_node_list5):
    if not isinstance(twdata, dict):
        twdata=twdata.__dict__
    rt_list1 = list()
    qt_list2 = list()
    try:
        if twdata.has_key('retweeted_status'):
            if not isinstance(twdata['retweeted_status'], dict):
                twdata['retweeted_status']=twdata['retweeted_status'].__dict__
            rt_list1 = get_data(twdata['retweeted_status'].copy())
    except Exception, e:
        user_defined_err_no = "5000001"
        get_traceback_info_of_exception(e, user_defined_err_no)
        sys.exit()

    try:
        if twdata.has_key('quoted_status'):
            if not isinstance(twdata['quoted_status'], dict):
                twdata['quoted_status']=twdata['quoted_status'].__dict__
            qt_list2 = get_data(twdata['quoted_status'].copy())
    except Exception, e:
        user_defined_err_no = "34gh"
        get_traceback_info_of_exception(e, user_defined_err_no)
        sys.exit()

    if rt_list1:
        try:
            type11 = rt_list1[0][4]
            if type11 == 'Tweet' or type11 == 'T_Mention':
                tweet_node_list1.append(rt_list1[0])
            if type11 == 'QuotedTweet' or type11 == 'QT_Mention':
                QTWEET_node_list2.append(rt_list1[0])
            if type11 == 'Reply' or type11 == 'R_Mention':
                ReplyTweet_node_list3.append(rt_list1[0])
            if type11 == 'Retweet':
                Retweet_node_list4.append(rt_list1[0])
            ##create mention_node csv file
            if rt_list1[1]:
                mention_node_list5.extend(rt_list1[1])
        except KeyError, e:
            # print "keyerror: ", str(e)
            user_defined_err_no = "sd56"
            get_traceback_info_of_exception(e, user_defined_err_no)
            logger.info("pass")
            pass

    if qt_list2:
        try:
            type11 = qt_list2[0][4]
            if type11 == 'Tweet' or type11 == 'T_Mention':
                tweet_node_list1.append(qt_list2[0])
            if type11 == 'QuotedTweet' or type11 == 'QT_Mention':
                QTWEET_node_list2.append(qt_list2[0])
            if type11 == 'Reply' or type11 == 'R_Mention':
                ReplyTweet_node_list3.append(qt_list2[0])
            if type11 == 'Retweet':
                Retweet_node_list4.append(qt_list2[0])
            ##create mention_node csv file
            if qt_list2[1]:
                mention_node_list5.extend(qt_list2[1])
        except KeyError, e:
            # print "keyerror: ", str(e)
            user_defined_err_no = "12fwe"
            get_traceback_info_of_exception(e, user_defined_err_no)
            logger.info("pass")
            pass







def create_csv_files_from_json(filename, tweets_d, neocsv_dir):
    logger.info("again started")
    global count
    count = 0
    tweet_node_list1 = list()
    tweet_node_list1.append(["Date", "TimeStamp", "TweetID", "Content", "Type", "Tokenize", "Valence", "Arousal", "Sentiment", "Emotion", "sec_nonsec_tag",  "retweet_source_id", "retweet_source_user_id", "replyto_source_id", "replyto_source_user_id", "quoted_source_id", "quoted_source_user_id", "like_count", "quote_count", "reply_count", "retweet_count", "tweet_lang", "tweet_location", "ID1", "UserName", "ScreenName", "user_Location", "Description", "followers_count", "listed_count", "statuses_count", "friends_count", "favourites_count"])

    QTWEET_node_list2 = list()
    QTWEET_node_list2.append(["Date", "TimeStamp", "TweetID", "Content", "Type", "Tokenize", "Valence", "Arousal", "Sentiment", "Emotion", "sec_nonsec_tag",  "retweet_source_id", "retweet_source_user_id", "replyto_source_id", "replyto_source_user_id", "quoted_source_id", "quoted_source_user_id", "like_count", "quote_count", "reply_count", "retweet_count", "tweet_lang", "tweet_location", "ID1", "UserName", "ScreenName", "user_Location", "Description", "followers_count", "listed_count", "statuses_count", "friends_count", "favourites_count"])

    ReplyTweet_node_list3 = list()
    ReplyTweet_node_list3.append(["Date", "TimeStamp", "TweetID", "Content", "Type", "Tokenize", "Valence", "Arousal", "Sentiment", "Emotion", "sec_nonsec_tag",  "retweet_source_id", "retweet_source_user_id", "replyto_source_id", "replyto_source_user_id", "quoted_source_id", "quoted_source_user_id", "like_count", "quote_count", "reply_count", "retweet_count", "tweet_lang", "tweet_location", "ID1", "UserName", "ScreenName", "user_Location", "Description", "followers_count", "listed_count", "statuses_count", "friends_count", "favourites_count"])

    Retweet_node_list4 = list()
    Retweet_node_list4.append(["Date", "TimeStamp", "TweetID", "Content", "Type", "Tokenize", "Valence", "Arousal", "Sentiment", "Emotion", "sec_nonsec_tag",  "retweet_source_id", "retweet_source_user_id", "replyto_source_id", "replyto_source_user_id", "quoted_source_id", "quoted_source_user_id", "like_count", "quote_count", "reply_count", "retweet_count", "tweet_lang", "tweet_location", "ID1", "UserName", "ScreenName", "user_Location", "Description", "followers_count", "listed_count", "statuses_count", "friends_count", "favourites_count"])

    mention_node_list5 = list()
    mention_node_list5.append(["TweetID", "ID1", "UserName", "ScreenName"])

    try:
        ##read the file and create csv files.............................1
        filepath = tweets_d+"/"+filename
        f= open(filepath, "r")
        try:
            json_data = json.load(f)
            for tweet in json_data:
                try:
                    tweet_dict = json.loads(json_data[tweet])
                    try:
                        temp_list = get_data(tweet_dict)
                        ##create csv files
                        try:
                            type11 = temp_list[0][4]
                            # print type11
                            if type11 == 'Tweet' or type11 == 'T_Mention':
                                tweet_node_list1.append(temp_list[0])
                            if type11 == 'QuotedTweet' or type11 == 'QT_Mention':
                                QTWEET_node_list2.append(temp_list[0])
                            if type11 == 'Reply' or type11 == 'R_Mention':
                                ReplyTweet_node_list3.append(temp_list[0])
                            if type11 == 'Retweet':
                                Retweet_node_list4.append(temp_list[0])

                            ##create mention_node csv file
                            if temp_list[1]:
                                mention_node_list5.extend(temp_list[1])
                        except KeyError, e:
                            user_defined_err_no = "2a"
                            get_traceback_info_of_exception(e, user_defined_err_no)
                            logger.info("pass")
                            pass
                        run_for_retweet_quoted_status(tweet_dict, tweet_node_list1, QTWEET_node_list2, ReplyTweet_node_list3, Retweet_node_list4, mention_node_list5)
                    except Exception, e:
                        ##  1) if deleted status came then it should be pass as because there is no tweet_id 'id_str'
                        ##  2) status may be like this also:  {u'limit': {u'track': 426, u'timestamp_ms': u'1540999425064'}}
                        pass
                        # print e
                        # sys.exit()
                except Exception, e:
                    user_defined_err_no = "2eer"
                    get_traceback_info_of_exception(e, user_defined_err_no)
                    sys.exit()
        except Exception, e:
                logger.info("Warning: File is not successfully created yet. After creating successfully, csv file generation will start...")
                # pass
                create_csv_files_from_json(filename, tweets_d, neocsv_dir)
                #sys.exit()

        f.close()
        ## write to csv file...............................................................2
        ##create folder with that file name.........2.a
        filename1 = filename.split(".")[0] # filename = "20181022_194629.json"
        try:
            # folder1 = "neo4j_csv/"+filename1
            folder1 = neocsv_dir+"/"+filename1
            os.mkdir(folder1)
        except Exception, e:
            user_defined_err_no = "2e"
            get_traceback_info_of_exception(e, user_defined_err_no)
            pass
        ##initialize file for writing.........2.b
        #tweet node csv file
        logger.info(folder1)
        tweet_node_file = open(folder1+"/1_tweet_node.csv", "a")
        tweet_node_writer = csv.writer(tweet_node_file, delimiter=",")        

        QTweet_node_file = open(folder1+"/2_qtweet_node.csv", "a")
        QTweet_node_writer = csv.writer(QTweet_node_file, delimiter=",")

        ReplyTweet_node_file = open(folder1+"/3_replyTweet_node.csv", "a")
        ReplyTweet_node_writer = csv.writer(ReplyTweet_node_file, delimiter=",")

        Retweet_node_file = open(folder1+"/4_retweet_node.csv", "a")
        Retweet_node_writer = csv.writer(Retweet_node_file, delimiter=",")

        #mention node csv file
        mention_node_file = open(folder1+"/5_mention_node.csv", "a")
        mention_node_writer = csv.writer(mention_node_file, delimiter=",")


        ##write to csv files.........
        tweet_node_writer.writerows(tweet_node_list1)
        QTweet_node_writer.writerows(QTWEET_node_list2)
        ReplyTweet_node_writer.writerows(ReplyTweet_node_list3)
        Retweet_node_writer.writerows(Retweet_node_list4)
        mention_node_writer.writerows(mention_node_list5)


        tweet_node_file.close()
        Retweet_node_file.close()
        QTweet_node_file.close()
        ReplyTweet_node_file.close()
        mention_node_file.close()

        f=open(tweets_d+"/"+filename1+".neocsv", "w")
        f.close()
        logger.info("completed")

        ## delete the (.start) file.....................................
        try:
            os.remove(tweets_d+"/"+filename1+".neostart")
            # os.remove(filepath+"/"+filename1+".neostart")
        except OSError, e:
            pass
    except ValueError, e:
        user_defined_err_no = "2rt"
        get_traceback_info_of_exception(e, user_defined_err_no)
        logger.info("pass")
        pass



def run(tweets_d, neocsv_dir):
    global current_file
    # logger.info("check")
    #list_of_files_with_json = list()
    list_of_files_with_neo = list()
    list_of_files_with_neostart = list()

    temp_l = os.listdir(tweets_d)
    temp_l.sort()
    ## take all .neo file name and store in a list
    for file1 in temp_l:
        if file1.endswith(".neocsv"):
            list_of_files_with_neo.append(file1.split(".")[0])
    ## take all .neostart file name and store in a list
    for file1 in temp_l:
        if file1.endswith(".neostart"):
            list_of_files_with_neostart.append(file1.split(".")[0])
    ## take all .json file name which does not have a .neo file name and thn store in a list
    for file1 in temp_l:
        if file1.endswith(".json"):
            fn = file1.split(".")[0]
            if fn not in list_of_files_with_neo and fn not in list_of_files_with_neostart:
                # list_of_files_with_json.append(fn)
                try:
                    ## take the file for insertion ...........................(.neostart)........empty file
                    f=os.open(tweets_d+"/"+fn+".neostart", os.O_CREAT | os.O_EXCL)
                    current_file = tweets_d+"/"+fn+".neostart"
                    os.close(f)
                except OSError, e:
                    ## whatever (.json) file choosen, if (.neostart) file for that (.json) file is present then abort this function to process further
                    if e.strerror == "File exists":
                        break
                logger.info("started for ----------------------------------------------> "+str(fn)+".json")
                create_csv_files_from_json(fn+".json", tweets_d, neocsv_dir)
                logger.info("completed for (total tweet= "+str(count)+")----------------------------------------------> "+str(fn)+".json")
                break



if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)

    '''check file is inserted or not...................................................1  remain
    get all files in name wise
    take all the .neo files
    take all the .json files which do not have .neo file
    take one file and do the things'''

    neocsv_dir = "/Users/debashisnaskar/Neo4j_Server/neo4j-community-3.5.5/import/neo4j_csv"
    ## take all file name and sort by their name and store in a list
    home_directory = os.environ['HOME']
    tweets_d = home_directory + "/Events/Master_Tweet_Json/track_tweets"
    while(1):
        run(tweets_d, neocsv_dir)