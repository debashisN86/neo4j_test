Steps of Member and follower crawling:

Before running the following functions, first start the cassandra.

#Start
cd Cassandra
bin/cassandra -f


1. First Run User_Member_Follower_Crawler.py by uncomment the following function "1_for crawling user info" to collect user information.
2. Comment "1_for crawling user info" >> Next Run "2_for crawling member list" from the same file called
User_Member_Follower_Crawler.py to collect member list of the user.
3. Comment "2_for crawling member list" >> Next Run "3__for crawling follower list" from the same
file called User_Member_Follower_Crawler.py to collect follower list of the user.

4. Then Run 2Csv_Program_Test.py to store the user information into CSV file.


5. To crawl tweet information, Run 3Crawl_By_TweetID.py.
     (No need to run 1_for crawling user info separately for retrieving user information)

6. Run 4Csv_Program_Test2 to get the final csv file with all user information.
      
N.B.: Optional (2. Continue, Comment "1_for crawling user info" >> Next Run "2_for crawling member 	list" from the same file called User_Member_Follower_Crawler.py to collect member list of the user.
	3. Comment "2_for crawling member list" >> Next Run "3__for crawling follower 		list" from the same file called User_Member_Follower_Crawler.py to collect follower list of the user.)


