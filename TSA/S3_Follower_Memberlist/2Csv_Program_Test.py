import csv
import tweepy
import time
import pprint, csv
import sys, codecs, json
from collections import OrderedDict
from itertools import combinations
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

reload(sys)
sys.setdefaultencoding('utf-8')


# set up connection to cluster....
cluster = Cluster(['127.0.0.1'], port=9042)  # local host
session = cluster.connect('tweet_keyspace')


class userInfoClass:
    def __init__(self):
        self.consumer_key = "DeCjdJeNZnyLXnAsAptnlkf3l"
        self.consumer_secret = "a3QRWUkHbksGxnxTvnf38FFv4ZliJit6LDyqHWwakuARzMubHt"
        self.access_token = "833992697157390336-1pZFbsRdDiokB8tXdQjdGmVXlTPHgfi"
        self.access_token_secret = "6UhIHY2xLwu4CsOIDdjDTOobXLo63BG1zXM1AG98XjJoh"

        self.auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.set_access_token(self.access_token, self.access_token_secret)
        self.api = tweepy.API(auth_handler=self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)    

    # -------------------------------USER INFO----------------------------------------------------
    # Gets User object for User(screen_name/id)
    def getUser(self, aid):
        return self.api.get_user(aid)

    # -------------------------------Checks if a friendship exists -----------------------------------------
    def frndship_btwen_2user(self, aid1, aid2):
        return self.api.show_friendship(source_id=aid1, target_id=aid2)

    # -------------------------------Checks if a lists_memberships exists ---------------------------------------
    def get_list(self, aid):
        for status in tweepy.Cursor(self.api.lists_memberships, id=aid).pages():
            pprint.pprint(status)

    # -------------------------------   Check if a user is a member of the specified list.
    def is_list_member1(self, owner, slug, aid):
        return self.api.is_list_member(owner, slug, aid)

        
def checking(file_name, file_path1):
    # open file for writing
    file2 = open(file_path1+file_name+"_new.csv", 'a')
    writer = csv.writer(file2, delimiter=',')
    writer.writerow(("Date", "TimeStamp", "User_ID", "Tweet_ID", "Sentiment", "Val", "Aro", "Sender", "Receiver", "Type"))

    u1 = userInfoClass()
    # time_series_row_list = list()

    # get the all user send tweets.......................................................................1
    count = 0
    user_sent_info_time_wise = OrderedDict()
    with open(file_name+".csv", "r") as f:
        csv_reader = csv.reader(f)
        for row in csv_reader:
            if row[8] == "receiver":
                continue
            else:
                # time_series_row_list.append(row)
                user_id = row[2]
                if user_id not in user_sent_info_time_wise:
                    user_sent_info_time_wise[user_id] = [row]
                else:
                    user_sent_info_time_wise[user_id].append(row)
    # with codecs.open('user_sent_info_time_wise_raw.json', 'w', encoding='utf8') as jf:
    # 	json.dump(user_sent_info_time_wise, jf, indent = 4, ensure_ascii=False)

    # get the all user who sent 2 tweets or more......................................................................2
    user_sent2_info_time_wise = OrderedDict()
    for u in user_sent_info_time_wise:
        if len(user_sent_info_time_wise[u]) >= 2:
            user_sent2_info_time_wise[u] = user_sent_info_time_wise[u]
    # with codecs.open('user_sent_info_time_wise_morethan2tweet.json', 'w', encoding='utf8') as jf:
    # 	json.dump(user_sent2_info_time_wise, jf, indent = 4, ensure_ascii=False)
    print "Total users", len(user_sent_info_time_wise)
    print "Total users who tweet more than 2 is: ", len(user_sent2_info_time_wise)

    #  store sent tweet count of all users who sent >= 2 tweets.......................................................3
    # tmp_dict = dict()
    # for k in user_sent2_info_time_wise:
    # 	tmp_dict[k] = len(user_sent2_info_time_wise[k])
    # sorted_dict = OrderedDict(sorted(tmp_dict.iteritems(), key=lambda (k,v): (v,k), reverse=True))
    # with codecs.open("user_sent_count_>=2tweet.json", 'w', encoding='utf8') as jf:
    # 	json.dump(tmp_dict, jf, indent = 4, ensure_ascii=False)

    # '''
    # get the all user's timestamp who sent 2 tweets or more.......................................................4
    user_sent2_info_time_wise = OrderedDict()
    for u in user_sent_info_time_wise:
        if len(user_sent_info_time_wise[u]) >= 2:
            tmp_list1 = list()
            for r in user_sent_info_time_wise[u]:
                tmp_list1.append(r)
            user_sent2_info_time_wise[u] = tmp_list1

    # ...................................................................................................5 checking
    # time_series_row_list.pop(0)
    count = 0
    for u in user_sent2_info_time_wise:
        time_series_row_list = list()
        with open(file_name+".csv", "r") as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                if row[8] == "receiver":
                    continue
                else:
                    time_series_row_list.append(row)
        time_series_row_list.pop(0)
        flag3 = False
        flag = False
        flag1 = True
        flag4 = False

        member_list_of_u = list()
        following_list_of_u = list()
        query = "SELECT author_id, member_of_lists1 from user_record_debashis where author_id=" + "'" + str(u) + "'"
        statement = SimpleStatement(query, fetch_size=1)
        rows = session.execute(statement)
        if rows:
            for (author_id, member_of_lists1) in rows:
                if member_of_lists1:
                    member_list_of_u = member_of_lists1
                else:
                    flag3 = True  # user's member list is not present
        else:
            flag = True  # if user is not present

        query = "SELECT author_id, following_list from user_record_debashis where author_id=" + "'" + str(u) + "'"
        statement = SimpleStatement(query, fetch_size=1)
        rows = session.execute(statement)
        if rows:
            for (author_id, following_list) in rows:
                if following_list:
                    following_list_of_u = following_list
                else:
                    flag4 = True  # user's friend list is not present
        else:
            flag = True  # if user is not present
        # print following_list_of_u

        # if count < 44:
        # 	print count, u
        # 	count += 1
        # 	continue
        timestamp_series_user_sent_tweet_list = list()
        print "\n\nFor User", count, ": ", u, ".................................................................", \
            len(user_sent2_info_time_wise[u]), "sent Tweet"
        for tmp_rows in user_sent2_info_time_wise[u]:
            timestamp_series_user_sent_tweet_list.append(tmp_rows[1])

        print timestamp_series_user_sent_tweet_list
        # for t in combinations(timestamp_series_user_sent_tweet_list, 2):
        for i in range(len(timestamp_series_user_sent_tweet_list) - 1):
            value = timestamp_series_user_sent_tweet_list[i:i+2]
            t1 = value[0]
            t2 = value[1]
            index_of_t1 = None
            index_of_t2 = None
            i1 = 0
            for row1 in time_series_row_list:
                if (t1 in row1) and (u in row1):
                    index_of_t1 = [i1]
                    break
                i1 += 1
            i2 = 0
            for row2 in time_series_row_list:
                if (t2 in row2) and (u in row2):
                    index_of_t2 = [i2]
                    break
                i2 += 1
            rows_between_t1_t2 = time_series_row_list[index_of_t1[0]:index_of_t2[0]]
            print "\n",t1,t2,len(rows_between_t1_t2), " tweets"

            for i22,r in enumerate(rows_between_t1_t2):
                if i22 == 0:
                    writer.writerow(r)  # write first row of rows_between_t1_t2
                    continue

                # check 2st condition (is the given user(r[2]) frnd with the user(u)).......................
                if not flag4:  # if frend_list doesn't exist for u
                    j1 = False
                    # print "entering to frnd list"
                    if int(r[2]) in following_list_of_u:
                        j1 = True
                    if j1: ##if j1 == True:
                        r[2] = u
                        r[7] = ""
                        r[8] = "followee"
                        writer.writerow(r)

                # check for 3rd condition (is this tweet came from subscribed list)..................................
                if flag3:   # if member_list doesn't exist for u
                    continue
                else:
                    member_list_of_u1 = set()
                    member_list_of_check_user1 = set()
                    member_list_of_check_user = list()
                    query = "SELECT author_id, member_of_lists1 from user_record_debashis where author_id=" + "'" + str(r[2]) + "'"
                    statement = SimpleStatement(query, fetch_size=1)
                    rows = session.execute(statement)
                    if rows:
                        for (author_id, member_of_lists1) in rows:
                            if member_of_lists1:
                                member_list_of_check_user = member_of_lists1

                        if member_list_of_check_user:
                            for m in member_list_of_u:
                                member_list_of_u1.update({m[0]})
                            if member_list_of_check_user[0]:
                                for m in member_list_of_check_user:
                                    member_list_of_check_user1.update({m[0]})
                            else:
                                continue

                            # now check for intersection
                            intersection_set = member_list_of_u1.intersection(member_list_of_check_user1)
                            if len(intersection_set) >= 1:
                                r[2] = u
                                r[7] = ""
                                r[8] = "memberlist"
                                # print "				a",i22, "------------------------ ", r[2], r[3], r[8]
                                writer.writerow(r)
            print "Finished for one pair of timestamp"

            if flag1:  # for last row of timestamp_series_user_sent_tweet_list
                if i == (len(timestamp_series_user_sent_tweet_list) - 2):
                    print "	", i, len(timestamp_series_user_sent_tweet_list) - 2,
                    writer.writerow(time_series_row_list[index_of_t2[0]])
                    # print "write successful 3 "
                    flag1 = False
            # if "Could not determine source user." is arise for u then stop all the process and
            # write all sent tweet if that user into csv file
            if flag:
                for i3,r in enumerate(user_sent2_info_time_wise[u]):
                    if i == 1:
                        if i3 <= 1:
                            continue
                    else:
                        if i3 == 0:
                            continue
                    writer.writerow(r)
                    # print "write successful 4"
                break

        count += 1
    # print time_series_row_list[:10]
    file2.close()
    # '''


if __name__ == '__main__':
    import glob
    count = 0
    # for file_path in glob.glob("/home/mala/twitter_cassandra_code/debashis_code/Case3/n/*.csv"):
    for file_path in glob.glob("/Users/debashisnaskar/PycharmProjects/TSA/S3_Follower_Memberlist/*.csv"):
        print file_path
        file_name = file_path.split("/")[-1].split('.')[0]
        print file_name
        # checking(file_name, '/home/mala/twitter_cassandra_code/debashis_code/Case3/n/New/')
        checking(file_name, '/Users/debashisnaskar/PycharmProjects/TSA/S3_Follower_Memberlist/New/')
        print count
        count += 1