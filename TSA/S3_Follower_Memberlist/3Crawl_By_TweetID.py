'''
crawl data by tid 
'''
import tweepy, json, sys, os, datetime, glob, csv
from tweepy.streaming import StreamListener
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
reload(sys)
sys.setdefaultencoding('utf-8')


def get_json_by_tid(api, tid):
    # return api.get_status(tid)
    return api.get_status(tid, tweet_mode='extended')
    #write_tweets(status._json)


def insert_tweet_into_cass(twdata):
    if not isinstance(twdata, dict):
        twdata=twdata.__dict__

    bundle_id1 = twdata['id_str']
    truncated = twdata['truncated']
    d1 = twdata['created_at']
    d2 = twdata['created_at']
    try:
        ##for date.......................
        if isinstance(d1, datetime.datetime):
            # print "..yes.."
            date = d1.date()
        else:
            # print "..no.."
            d1 = datetime.datetime.strptime(d1, "%a %b %d %H:%M:%S +0000 %Y")
            date = d1.date()
            #time = d1.time()
    except AttributeError, e:
        print "1"
        # raise
    if truncated:
        extended_tweet_dict = twdata['extended_tweet']
        tweet_text = extended_tweet_dict['full_text']
        entities = extended_tweet_dict['entities']
        ##for #tag..............
        if entities.has_key('hashtags'):
            hashtags_list = entities['hashtags']  ## in list format from crawler
            hashtags_list1 = []
            for h in hashtags_list:
                ht = h['text']
                hashtags_list1.append(ht)
        ##for mentions...........
        if entities.has_key('user_mentions'):
            mentions_list = entities['user_mentions']  ## in list format from crawler
            mentions_list1 = []
            mentions_list_id = []
            for m in mentions_list:
                sn = m['screen_name']
                mentions_list1.append(sn)
                mentions_list_id.append(m['id_str'])
    else:
        tweet_text = twdata['full_text']
        ##for #tag..............
        if twdata['entities'].has_key('hashtags'):
            hashtags_list = twdata['entities']['hashtags']  ## in list format from crawler
            hashtags_list1 = []
            for h in hashtags_list:
                ht = h['text']
                hashtags_list1.append(ht)
        ##for mentions...........
        if twdata['entities'].has_key('user_mentions'):
            mentions_list = twdata['entities']['user_mentions']
            mentions_list1 = []
            mentions_list_id = []
            for m in mentions_list:
                sn = m['screen_name']
                mentions_list1.append(sn)
                mentions_list_id.append(m['id_str'])

    is_quote_status = twdata['is_quote_status']
    ##if tweet is a retweet or quoted or reply or tweet........
    try:
        type1 = 'Tweet'
        quoted_source_id = None
        replyTo_source_id = None
        replyTo_user_id = None
        replyTo_screen_name = None
        retweet_source_id = None

        ##if tweet is a retweet of a TWEET........
        if twdata.has_key('retweeted_status'):
            if not isinstance(twdata['retweeted_status'], dict):
                twdata['retweeted_status']=twdata['retweeted_status'].__dict__
            type1 = 'retweet'
            retweet_source_id = twdata['retweeted_status']['id_str']  #original tweet ID
            #print '1re'+retweet_source_id
            #write_in_cassandra(twdata['retweeted_status'].copy())
            quoted_source_id = None
            replyTo_source_id = None
            replyTo_user_id = None
            replyTo_screen_name = None
        ##if tweet is a "RT of QT" and "QT"........
        if twdata['is_quote_status']:
            if not twdata.has_key('quoted_status'):
                try:
                    quoted_source_id1 = twdata['quoted_status_id_str']#original tweet ID
                    id1 = quoted_source_id1 + '\n'
                except Exception, e:
                    print '3487687'
            ##if tweet is a retweet of a QTWEET........
            if twdata.has_key('retweeted_status'):
                if not isinstance(twdata['retweeted_status'], dict):
                    twdata['retweeted_status']=twdata['retweeted_status'].__dict__
                try:
                    type1 = 'retweet'
                    retweet_source_id = twdata['retweeted_status']['id_str']  #original tweet ID
                    quoted_source_id = None
                    replyTo_source_id = None
                    replyTo_user_id = None
                    replyTo_screen_name = None
                except Exception, e:
                    print '234'
            else:
                try:
                    ##if tweet is a quoted........
                    type1 = 'QuotedTweet'
                    quoted_source_id = twdata['quoted_status_id_str']#original tweet ID
                    retweet_source_id = None
                    replyTo_source_id = None
                    replyTo_user_id = None
                    replyTo_screen_name = None
                except Exception, e:
                    ## id 'is_quote_status' true but 'quoted_status_id_str' not present then treat that QT as a Tweet
                    type1 = 'Tweet'
                    quoted_source_id = None
                    replyTo_source_id = None
                    replyTo_user_id = None
                    replyTo_screen_name = None
                    retweet_source_id = None
                    # print str(234234234)
                    print "sjh22"
                    # sys.exit()
        ##if tweet is a reply........
        if twdata['in_reply_to_status_id_str'] is not None:
            #if twdata['in_reply_to_status_id_str'] is not None:
            type1 = 'Reply'
            if twdata.has_key('in_reply_to_status_id_str'):
                replyTo_source_id = twdata['in_reply_to_status_id_str']      #original tweet ID
            if twdata.has_key('in_reply_to_user_id_str'):
                replyTo_user_id = twdata['in_reply_to_user_id_str']
            if twdata.has_key('in_reply_to_screen_name'):
                replyTo_screen_name = twdata['in_reply_to_screen_name']
            quoted_source_id = None
            retweet_source_id = None
        #only tweet...........
        if not (twdata.has_key('retweeted_status') or twdata.has_key('quoted_status') or is_quote_status):
            type1 = 'Tweet'
            quoted_source_id = None
            replyTo_source_id = None
            replyTo_user_id = None
            replyTo_screen_name = None
            retweet_source_id = None
    except Exception, e:
        print '2'

    #for lang and keywords...............
    if twdata['lang'] is not None:
        lang = twdata['lang']


    #for author_info............
    try:
        user_dict=twdata['user']
        if not isinstance(twdata['user'], dict):
            user_dict=twdata['user'].__dict__
        author_id = user_dict['id_str']
        geo = user_dict['location']
        ##store user info............................
        write_to_table_user_record(user_dict, None)
    except Exception, e:
        # logger.info(str(sys.exc_traceback.tb_lineno))
        print 'i89'
        sys.exit()

    try:
        bound = prepared_tweet_info_table.bind((twdata['id_str'], d1, hashtags_list1, lang, twdata['favorite_count'], geo,  mentions_list1, mentions_list_id, None, quoted_source_id, None, replyTo_screen_name, replyTo_source_id, replyTo_user_id, twdata['retweet_count'], retweet_source_id, tweet_text, type1, author_id))
        session.execute(bound)
        print "..............Inserted to tweetinfo_id"
    except Exception, e:
        print str(e)
        print '301212'


def write_to_table_user_record(user_object_json, member_of_lists):
        d1 = user_object_json['created_at']
        # print user_object_json
        try:
            # for date.......................
            # print d1
            if isinstance(d1, datetime.datetime):
                # print "..yes.."
                date = d1.date()
            else:
                # print "..no.."
                d1 = datetime.datetime.strptime(d1, "%a %b %d %H:%M:%S +0000 %Y")
                date = d1.date()
                # time = d1.time()
        except AttributeError, e:
            print str(1)
            raise

        utc_offset = None
        if user_object_json['utc_offset']:
            utc_offset = str(user_object_json['utc_offset'])

        try:
            bound = prepared_user_record.bind((user_object_json['id_str'], user_object_json['name'], user_object_json['screen_name'], d1, user_object_json['description'], user_object_json['favourites_count'], user_object_json['followers_count'], user_object_json['friends_count'], None, user_object_json['lang'], user_object_json['listed_count'], user_object_json['location'], member_of_lists, user_object_json['profile_background_image_url_https'], user_object_json['profile_image_url_https'], user_object_json['protected'], user_object_json['time_zone'], user_object_json['statuses_count'], user_object_json['url'], utc_offset, user_object_json['verified']))
            session.execute(bound)
            print "...............................................Inserted to user_record"
        except Exception, e:
            print str(31)
            print str(e)
            sys.exit()


if __name__ == '__main__':
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('tweet_keyspace')

    try:
        query='INSERT INTO tweet_record_debashis (tid, datetime, hashtags, lang, like_count, location, mentions, mentions_id, quote_count, quoted_source_id, reply_count, replyto_screen_name, replyto_source_id, replyto_user_id, retweet_count, retweet_source_id, tweet_text, type, uid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        prepared_tweet_info_table = session.prepare(query)
    except Exception, e:
        print str(e)
        print "12"
        sys.exit()

    try:
        query1 = 'INSERT INTO user_record_debashis(author_id, author, author_screen_name, created_at, description, ' \
                'favourites_count, followers_count, following_count, following_list, lang, listed_count, location, ' \
                'member_of_lists1, profile_background_image_url_https, profile_image_url_https, protected, ' \
                'time_zone, tweet_count, url, utc_offset, verified) VALUES' \
                ' (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        prepared_user_record = session.prepare(query1)
    except Exception, e:
        print "892 Error in prepare statement block\n"
        print str(e)
        sys.exit()

    consumer_key = "DEaxDN2GuaVpsWjjGYvZBDyzI"
    consumer_secret = "7ZnGQ6tL6Nuv1BQ2c1n3foq1TbMdxso3qfsJq0f9XZzppgpZNs"
    access_token = "996264862899625984-IYKDwiDF1cnrzVqqJfxAJZRsaKPJ5bf"
    access_token_secret = "F9FjljO1A9a4XD807FVBDyTlQph0XMVcNtRbYJseXVbYX"
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


    # tid_list = ['1060365191722938368']
    for file_path in glob.glob("/Users/debashisnaskar/PycharmProjects/TSA/S3_Follower_Memberlist/New/*.csv"):
        print file_path
        tid_list = dict()
        c = 0
        with open(file_path, "r") as f:
            csv_reader = csv.reader(f)
            for r in csv_reader:
                if c == 0:
                    c += 1
                    continue
                if r[8] != '':
                    # print r[3], r[8]
                    if r[3] not in tid_list:
                        tid_list[r[3]] = 1
                    else:
                        tid_list[r[3]] += 1
                c += 1

        # print tid_list
        for tid in tid_list:
            print tid
            query = "SELECT * from tweet_record_debashis WHERE tid='"+tid+"'"
            rows = session.execute(query)
            if rows:
                continue
            else:
                print "not crawled yet"
            try:
                json_object = get_json_by_tid(api, tid)
                data_j = json_object._json
                # print data_j
                insert_tweet_into_cass(data_j)
            except tweepy.error.TweepError as e:
                ##error1 : [{u'message': u'No status found with that ID.', u'code': 144}]
                #l = e.api_code
                #l = e.message[0]['message']
                print e