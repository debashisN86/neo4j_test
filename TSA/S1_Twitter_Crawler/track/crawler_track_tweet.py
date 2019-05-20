'''
created: 2018-10-16
Info : 
    crawl streaming data and store into json file as a bunch of 10000 tweets in a file
'''
import tweepy, time, json, sys, datetime, traceback, pprint, logging, os, atexit
from tweepy.streaming import StreamListener
from collections import OrderedDict
reload(sys)
sys.setdefaultencoding('utf-8')

# Create and configure logger
logging.basicConfig(filename="crawler_track_tweet.log", format='INFO %(asctime)s %(message)s', filemode='a')
#  Creating an object
logger = logging.getLogger()
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.INFO) 
# logger.info("5698999")

# initialization..........
tweet_count = 0
datetime1 = datetime.datetime.utcnow()
d1 = str(datetime1).split(".")[0].replace("-", "").replace(":", "").replace(" ", "_")
filename = str(d1)+"_t.json"
final_dict = OrderedDict()


class MyStreamListener(StreamListener):	

    consumer_key = "23rgGpLd68v702zzfQjOn56Cy"
    consumer_secret = "Qd0lJOlRSFGI7R8FyqpKaN7ZqSg8UWMWgMNcA0j0BfNHK7g7om"
    access_token = "2778508951-linVGgo8SpaaQ6aJwZzk2k827BmEHnaJ0BwMYQ3"
    access_token_secret = "wiyVbbafaBZUwwPumeLsYxQx09ZsYzu79leL6oePh1ffW"

    # Sets up connection with OAuth
    def __init__(self):
        auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_token_secret)
        self.api = tweepy.API(auth)

    def on_data(self, data):
        try:
            write_tweets(data)
        except Exception, e:
            logger.info("Error in on_data()------->")
            # traceback.print_exc(file=sys.stdout)
            logger.info(str(datetime.datetime.now()))
            logger.info(str(sys.exc_info()[0]))
            logger.info("Error ===================>")
            logger.info(str(e))

    def on_error(self, status_code):
        logger.info("Error in on_error()------->")
        # logger.info(datetime.datetime.now(),status_code)
        if status_code == 420:
            # returning False in on_data disconnects the stream
            logger.info('420')
            return False

    def on_limit(self, status):
        logger.info("Error in on_limit()------->")
        logger.info('Limit threshold exceeded'+str(status))

    def on_timeout(self, status):
        logger.info("Error in on_timeout()------->")
        logger.info('Stream disconnected; continuing...')


class twitterHelper:
    consumer_key = "23rgGpLd68v702zzfQjOn56Cy"
    consumer_secret = "Qd0lJOlRSFGI7R8FyqpKaN7ZqSg8UWMWgMNcA0j0BfNHK7g7om"
    access_token = "2778508951-linVGgo8SpaaQ6aJwZzk2k827BmEHnaJ0BwMYQ3"
    access_token_secret = "wiyVbbafaBZUwwPumeLsYxQx09ZsYzu79leL6oePh1ffW"

    # Sets up connection with OAuth
    def __init__(self):
        auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_token_secret)
        self.api = tweepy.API(auth)

    def getStreamTweets(self, topicList):
        try:
            logger.info(topicList)
            myStreamListener = MyStreamListener()
            myStream = tweepy.Stream(auth=self.api.auth, listener=myStreamListener)
            myStream.filter(track=topicList)
        except Exception, e:
            logger.info("Error in getStreamTweets()------->")
            logger.info(str(e))


def exit_handler():
    global final_dict, filename, tweets_d
    if len(final_dict) > 100:
        datetime1 = datetime.datetime.utcnow()
        d1 = str(datetime1).split(".")[0].replace("-", "").replace(":", "").replace(" ", "_")
        filename = str(d1)+"_t.json"
        f = open(tweets_d+filename, 'w')
        json.dump(final_dict, f, ensure_ascii=False, indent=4)
        f.close()
        logger.info('My application connection broken')


# Writes all tweets of a user to a file in json format
def write_tweets(twdata):  
    global tweet_count, final_dict, filename, tweets_d
    if tweet_count == 2000:  # to store data
        f = open(tweets_d+filename, 'w')
        json.dump(final_dict, f, ensure_ascii=False, indent=4)
        f.close()
        # initialization..........
        tweet_count = 0
        datetime1 = datetime.datetime.utcnow()
        d1 = str(datetime1).split(".")[0].replace("-", "").replace(":", "").replace(" ", "_")
        filename = str(d1)+"_t.json"
        final_dict = OrderedDict()

    else:
        if tweet_count % 1000 == 0:  # to check the log fle
            str11 = "Tweet Count: "+str(tweet_count)
            logger.info(str11)
        # logger.info(tweet_count)
        final_dict.update({tweet_count: twdata})
        tweet_count += 1


if __name__ == '__main__':
    # get home directory....
    home_directory = os.environ['HOME']
    tweets_d = home_directory + "/Events/Master_Tweet_Json/track_tweets/"

    t=twitterHelper()
    topicList = []

    # get the track list from file if any...............................................................5
    fd1 = open('track_list.txt', 'r')
    for line in fd1 :
        topicList.append(line.strip())
    fd1.close()

    print topicList

    # start
    logger.info("Streaming Started ---------------------------------- > ")
    t.getStreamTweets(topicList)

    atexit.register(exit_handler)
