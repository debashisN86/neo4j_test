import csv, glob, sys
from cassandra.cluster import Cluster
reload(sys)
sys.setdefaultencoding('utf-8')


def generate(file_name, file_path1, file_path_read):
    # open file for writing
    file2 = open(file_path1+file_name+"_new1.csv", 'w')
    writer = csv.writer(file2, delimiter=',')
    writer.writerow(("Date", "TimeStamp", "User_ID", "Tweet_ID", "Sentiment", "Val", "Aro", "Sender", "Receiver", "Type", "User_ID_Receiver"))

    c=0
    with open(file_path_read, "r") as f:
        csv_reader = csv.reader(f)
        for r in csv_reader:
            if c == 0:
                c += 1
                continue
            if r[8] != '':
                tid = r[3]
                query = "SELECT * from tweet_record_debashis WHERE tid='"+tid+"'"
                rows = session.execute(query)
                if rows:
                    for row in rows:
                        Receiver_aid = row.uid
                        break
                    r.append(Receiver_aid)
                else:
                    r.append('NONE') ## status not found
            else:
                # r.append(r[2]) ##sender
                r.append('NA')
            c += 1
            writer.writerow(r)


if __name__ == '__main__':
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('tweet_keyspace')
    count = 0
    # for file_path in glob.glob("/home/mala/twitter_cassandra_code/debashis_code/Case3/n/*.csv"):
    for file_path in glob.glob("/Users/debashisnaskar/PycharmProjects/TSA/S3_Follower_Memberlist/New/*.csv"):
        print file_path
        file_name = file_path.split("/")[-1].split('.')[0]
        print file_name
        # checking(file_name, '/home/mala/twitter_cassandra_code/debashis_code/Case3/n/New/')
        generate(file_name, '/Users/debashisnaskar/PycharmProjects/TSA/S3_Follower_Memberlist/New/Receiver_UserID/', file_path)
        print count
        count += 1