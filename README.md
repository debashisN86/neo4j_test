# neo4j_test

## code dir on Debo
  path : /Users/debashisnaskar/PycharmProjects/neo4j_test
## To run
  1) Go to path /Users/debashisnaskar/PycharmProjects/neo4j_test/TSA/S1_Twitter_Crawler/ingestion
  
  2) conda activate crawlerEnv [Activate conda path for python2] 
  
  3) python python import_csv_to_neo_hemanta.py [ to import raw csv to ne4j database]
 
## Neo4j Server commands
### to clear database
  1) MATCH (u:USER) DETACH DELETE u
  2) MATCH (u:MESSAGE) DETACH DELETE u
  
### to drop constraints
  1) DROP CONSTRAINT ON ( message:MESSAGE ) ASSERT message.TweetID IS UNIQUE
  2) DROP CONSTRAINT ON ( user:USER ) ASSERT user.UserId IS UNIQUE
  
### to view all constraints

     call db.constraints
  
 ## Neo4j home dir location
   1) raw csv is in : /Users/debashisnaskar/Neo4j_Server/neo4j-community-3.5.5/import/neo4j_csv
   
   
  
