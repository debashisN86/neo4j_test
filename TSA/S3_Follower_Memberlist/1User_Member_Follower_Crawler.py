"""
crawl user info by id (get user info from "user_inserted_at" table by date)
"""

import time
import tweepy
import json
import sys
import unicodedata, glob
import datetime
import pprint, csv

from requests.exceptions import ProxyError
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
reload(sys)
sys.setdefaultencoding('utf-8')


# utc time
today_datetime = datetime.datetime.utcnow()
utc_today_date = today_datetime.date()
utc_yes_dateime = today_datetime + datetime.timedelta(-2)
# utc_yes_date = utc_yes_dateime.date()

utc_yes_date = '2018-04-11'

# local time
today_datetime = datetime.datetime.now()
local_today_date = today_datetime.date()


class userInfoClass:
    def __init__(self):
        self.consumer_key = "23rgGpLd68v702zzfQjOn56Cy"
        self.consumer_secret = "Qd0lJOlRSFGI7R8FyqpKaN7ZqSg8UWMWgMNcA0j0BfNHK7g7om"
        self.access_token = "2778508951-linVGgo8SpaaQ6aJwZzk2k827BmEHnaJ0BwMYQ3"
        self.access_token_secret = "wiyVbbafaBZUwwPumeLsYxQx09ZsYzu79leL6oePh1ffW"

        self.auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.set_access_token(self.access_token, self.access_token_secret)
        self.api = tweepy.API(auth_handler=self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)    

    # -------------------------------USER INFO----------------------------------------------------
    # Gets User object for User(screen_name/id)
    def getUser(self, aid):
        return self.api.get_user(aid)

    # ----------------------------FOLLOWERS-------------------------------------------------------
    # #Get User objects of all the followers of the user specified by aid
    # def getFollowers_object(self, aid):
    #     followerList = []
    #     # cur = tweepy.Cursor(api.followers, id=aid).items()
    #     for status in tweepy.Cursor(self.api.followers, id=aid).pages():
    #     	# pprint.pprint(status)
    #     	print len(status)

    def getFollowers_ids(self, aid):
        followerList = []
        # cur = tweepy.Cursor(api.followers, id=aid).items()
        count = 1
        for status in tweepy.Cursor(self.api.followers_ids, id=aid).items():  # per page a list of 5000 id's are given
            print count
            pprint.pprint(status)
            count = count + 1
            # print len(status)

    # ### ----------------------------FOLLOWING--------------------------------------------------------
    # #Get User objects of all the following of the user specified by aid
    # def getFollowing_object(self, aid):
    #     followingList = []
    #     for status in tweepy.Cursor(self.api.friends, id=aid).items(1):
    #     	pprint.pprint(status._json)
    #     	# print len(status)

    # Get User objects of all the followig of the user specified by aid
    def getFollowing_ids(self, aid):        
        followingList = []
        for status in tweepy.Cursor(self.api.friends_ids, id=aid, count=1000).items():
            # pprint.pprint(status)
            followingList.append(status)
            # print len(status)
        return followingList

    # -------------------------------lists_memberships ----------------------------------------------------
    def get_list(self, aid1):
        list1 = list()
        for status in tweepy.Cursor(self.api.lists_memberships, id=aid1, count=995).items():
            # pprint.pprint(status)
            list1.append([status.id_str, status.slug, status.user._json["screen_name"], status.user._json["id_str"]])
        return list1


class writeToCassandra:
    # inserted to table user_record......................................
    def write_to_table_user_record(self, user_object_json, member_of_lists):
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


def crawl_user_info(file_path):
    u = userInfoClass()
    w = writeToCassandra()
    # select query on  user_inserted_at
    try:
        aid_dict = dict()
        c=0
        with open(file_path, "r") as f:
            csv_reader = csv.reader(f)
            for r in csv_reader:
                # print r
                if c == 0:
                    c += 1
                    continue
                if r[7] == "sender":
                    # print r[7]
                    if r[2] not in aid_dict:
                        aid_dict[r[2]] = 1
                    else:
                        aid_dict[r[2]] += 1
                c+=1

        count = 0
        print len(aid_dict)

        # for user crawling.......................................
        for d in aid_dict:
            try:
                query = "SELECT * FROM user_record_debashis WHERE author_id=" + "'" + str(d) + "'"
                rows = session.execute(query)
                if rows:
                    print "User ", d, "already present"
                    continue
                else:
                    try:
                        st = u.getUser(int(d))
                        user_object_json = st._json
                        print "Author Id {0}------------------------------- {1} ".format(d, count)
                        count = count + 1
                        w.write_to_table_user_record(user_object_json, None)
                    except tweepy.error.TweepError as e:
                        # error2 : [{u'message': u'User not found.', u'code': 50}]
                        print "AAuthor Id {0}------------------------------- {1}   909090 ------------------------->" \
                              "   TweepError".format(d, count)
                        # l = e.api_code
                        # l = e.message[0]['message']
                        print e
                        count = count + 1
                        # sys.exit()
                    except ProxyError, e:
                        print 'AAuthor Id  {0}----------------------------------------- {1}   808080 ' \
                              '------------------------->   ProxyError'.format(d, count)
                        print str(e)
                        flag = True
                        while(flag):
                            try:
                                st = u.getUser(int(d))
                                user_object_json = st._json
                                w.write_to_table_user_record(user_object_json, None)
                                count = count + 1
                                flag = False
                            except tweepy.error.TweepError as e:
                                print 'AAuthor Id  {0}----------------------------------------- {1}    707070  ' \
                                      '------------------------->   TweepError'.format(d, count)
                                count = count + 1
                                flag = False
                            except ProxyError, e:
                                print 'AAuthor Id  {0}----------------------------------------- {1}    606060 ' \
                                      ' ------------------------->   ProxyError'.format(d, count)
                                print str(e)
                                flag = True
            except Exception, e:
                print "Error 456790"
                print str(e)
                sys.exit()
        # ......................................................END...............................................

        # u.get_list(2778508951)
        # pprint.pprint(st._json)

        # list1 = [848879078971236400, 2236860644, 2744285933, 2746869502, 555207122, 64308610, 2920181586,
                # 856400589449601000, 819314087431532500, 1586847469, 2288567624, 3083610668, 316718000]
        # for c in list1:
        # 	try:
        # 		st = u.getUser(c)
        # 		user_object_json = st._json
        # 		w.write_to_table_user_record(user_object_json, None)
        # 	except Exception, e:
        # 		print str(e)
    except Exception, e:
        print "1Error in selecting\n"
        print str(e)
        sys.exit()


def crawl_user_memberlists():
    u = userInfoClass()
    query = "SELECT author_id, listed_count, member_of_lists1 from user_record_debashis"
    statement = SimpleStatement(query, fetch_size=1000)
    for (author_id, listed_count, member_of_lists1) in session.execute(statement):
        try:
            if listed_count > 0:
                if member_of_lists1 is None:
                    print author_id, listed_count, member_of_lists1
                    list1 = u.get_list(author_id)
                    # print list1

                    # query = "UPDATE user_record_debashis SET member_of_lists=%s WHERE author_id=%s"
                    session.execute("""UPDATE user_record_debashis SET member_of_lists1=%s WHERE author_id=%s""", (list1, author_id))
        except tweepy.error.TweepError as e:
                # error2 : [{u'message': u'User not found.', u'code': 50}]
                print "{0}   909090 ------------------------->   TweepError".format(author_id)
                # l = e.api_code
                # l = e.message[0]['message']
                print e
                # sys.exit()
    # list1 = u.get_list(363534196)
    # print list1


def users_following_list_ids():
    u = userInfoClass()
    w = writeToCassandra()

    try:
        count = 0
        query = "SELECT * FROM user_record_debashis"
        rows = session.execute(query)
        for r in rows:
            following_list = r.following_list
            followig_count = r.following_count
            d = r.author_id
            if followig_count <= 50000:
                if following_list:
                    print "User ", d, "already present", count
                    count = count + 1
                    continue
                else:
                    try:
                        following_list_ids = u.getFollowing_ids(int(d))
                        print "Author Id {0}------------------------------- {1}, {2} ".format(d, followig_count, count)
                        count = count + 1
                        session.execute("""UPDATE user_record_debashis SET following_list=%s WHERE author_id=%s""",
                                        (following_list_ids, d))
                    except tweepy.error.TweepError as e:
                        # error2 : [{u'message': u'User not found.', u'code': 50}]
                        print "AAuthor Id {0}------------------------------- {1}   909090 -------------------------> " \
                              "  TweepError".format(d, count)
                        # l = e.api_code
                        # l = e.message[0]['message']
                        print e
                        count = count + 1
                        # sys.exit()
                    except ProxyError, e:
                        print 'AAuthor Id  {0}----------------------------------------- {1}   808080 ' \
                              '------------------------->   ProxyError'.format(d, count)
                        print str(e)
                        flag = True
                        while(flag):
                            try:
                                following_list_ids = u.getFollowing_ids(int(d))
                                session.execute("""UPDATE user_record_debashis SET following_list=%s WHERE author_id=%s""",
                                                (following_list_ids, d))
                                count = count + 1
                                flag = False
                            except tweepy.error.TweepError as e:
                                print 'AAuthor Id  {0}----------------------------------------- {1}    707070 ' \
                                      ' ------------------------->   TweepError'.format(d, count)
                                count = count + 1
                                flag = False
                            except ProxyError, e:
                                print 'AAuthor Id  {0}----------------------------------------- {1}    606060 ' \
                                      ' ------------------------->   ProxyError'.format(d, count)
                                print str(e)
                                flag = True
    except Exception, e:
        print "Error 456790"
        print str(e)
        sys.exit()


def users_following_list_ids_new():
    # To run large number of user.
    u = userInfoClass()
    w = writeToCassandra()

    try:
        count = 0
        with open("u1.txt") as f:
            data_r = f.readlines()

        for d in data_r:
            print d
            d = d.strip("\n")
            query = "SELECT * FROM user_record_debashis WHERE author_id=" + "'" + str(d) + "'"
            rows = session.execute(query)
            if rows:
                for r in rows:
                    following_list = r.following_list
                    followig_count = r.following_count
                    d = r.author_id
                    if followig_count <= 50000:
                        if following_list:
                            print "User ", d, "already present", count
                            count = count + 1
                            continue
                        else:
                            try:
                                following_list_ids = u.getFollowing_ids(int(d))
                                print "Author Id {0}------------------------------- {1}, {2} ".format(d, followig_count, count)
                                count = count + 1
                                session.execute("""UPDATE user_record_debashis SET following_list=%s WHERE author_id=%s""",
                                                (following_list_ids, d))
                            except tweepy.error.TweepError as e:
                                # error2 : [{u'message': u'User not found.', u'code': 50}]
                                print "AAuthor Id {0}------------------------------- {1}   909090 " \
                                      "------------------------->   TweepError".format(d, count)
                                #l = e.api_code
                                # l = e.message[0]['message']
                                print e
                                count = count + 1
                                # sys.exit()
                            except ProxyError, e:
                                print 'AAuthor Id  {0}----------------------------------------- {1}   808080 ' \
                                      '------------------------->   ProxyError'.format(d, count)
                                print str(e)
                                flag = True
                                while(flag):
                                    try:
                                        following_list_ids = u.getFollowing_ids(int(d))
                                        session.execute("""UPDATE user_record_debashis SET following_list=%s WHERE author_id=%s""",
                                                        (following_list_ids, d))
                                        count = count + 1
                                        flag = False
                                    except tweepy.error.TweepError as e:
                                        print 'AAuthor Id  {0}----------------------------------------- {1}    707070' \
                                              '  ------------------------->   TweepError'.format(d, count)
                                        count = count + 1
                                        flag = False
                                    except ProxyError, e:
                                        print 'AAuthor Id  {0}----------------------------------------- {1}    606060' \
                                              '  ------------------------->   ProxyError'.format(d, count)
                                        print str(e)
                                        flag = True
    except Exception, e:
        print "Error 456790"
        print str(e)
        sys.exit()


if __name__ == '__main__':

    # date.................................................................
    datetime_now = datetime.datetime.now()
    date_now = datetime_now.date()

    # set up connection to cluster....
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('tweet_keyspace')

    # to prepare statement for inserting into user_record...........................................4
    try:
        query = 'INSERT INTO user_record_debashis(author_id, author, author_screen_name, created_at, description, ' \
                'favourites_count, followers_count, following_count, following_list, lang, listed_count, location, ' \
                'member_of_lists1, profile_background_image_url_https, profile_image_url_https, protected, ' \
                'time_zone, tweet_count, url, utc_offset, verified) VALUES' \
                ' (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        prepared_user_record = session.prepare(query)
        prepared_user_record.consistency_level = ConsistencyLevel.ONE
    except Exception, e:
        print "892 Error in prepare statement block\n"
        print str(e)
        # cluster.shutdown()
        sys.exit()

    # 1_for crawling user info
    # for file_path in glob.glob("/Users/debashisnaskar/PycharmProjects/TSA/S3_Follower_Memberlist/*.csv"):
    #     print file_path
    #     crawl_user_info(file_path)

    # 2_for crawling member list
    # crawl_user_memberlists()

    # 3__for crawling follower list
    # users_following_list_ids()