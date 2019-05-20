# to activate python 2.7
$ conda activate <env name> # such as crawlerEnv 
$ conda deactivate # to exit from environment
$ conda env list # to check the list

# To run the crawler (Backend process)
1. Run track (sh continue_crawler_track_tweet.sh &)
2. Run handle (sh continue_crawler_handle_tweet.sh &)
3. To kill a process --
$ ps -ef | grep "script name" # such as "continue_crawler_handle_tweet.sh"

$ kill -9 <pid> #second id is process id 

# To Run crawler, create csv and import to neo4j together (Pycharm)





# To check the log file

$ tail -f <log_file> # such as neo4j_create_csv_handle_1.log