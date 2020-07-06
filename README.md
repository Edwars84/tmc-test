# Preparation
from {/file/to/project}/src/main/resources/docker execute

`docker-compose up`

also add line 

`172.27.1.5      namenode` 

to /etc/hosts

# Questions 

## 1. Hive and Hdfs.

### 1.1. Load the csv attachment in hdfs.

`docker cp /home/edu/IdeaProjects/tmc-test/src/main/resources/CSV-Prueba.csv namenode:/home/CSV-Prueba.csv`

`docker exec -it namenode hdfs dfs -mkdir /test`

`docker exec -it namenode hdfs dfs -put /home/CSV-Prueba.csv /test/CSV-Prueba.csv`

### 1.2.Create table in hive.

`docker exec -it hive-server /bin/bash`

`beeline -u jdbc:hive2://localhost:10000 -n hive -p hive`

`CREATE EXTERNAL TABLE IF NOT EXISTS external_csv (member_id INT, name STRING, email STRING, joined BIGINT, ip_address STRING, posts INT, bday_day INT, bday_month INT, bday_year INT, members_profile_views INT, referred_by INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  STORED AS TEXTFILE LOCATION '/test';`

`CREATE TABLE IF NOT EXISTS csv_test (member_id INT, name STRING, email STRING, joined BIGINT, ip_address STRING, posts INT, bday_day INT, bday_month INT, bday_year INT, members_profile_views INT, referred_by INT) STORED AS ORC;`

`INSERT INTO TABLE csv_test SELECT * FROM external_csv;`

### 1.3.Hive

#### 1.3.1. Generate a query to obtain a most birthdays on 1 day

`select bday_day, bday_month, count(1) as num_bdays from external_csv group by bday_month, bday_day order by num_bdays desc limit 1;`

#### 1.3.2. Generate a query to obtain Least birthdays are on 11 months

 I'm very sorry but I don't understand this question
