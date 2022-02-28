# Project 3: Understanding User Behavior
Team members: Rochelle Li, Muhammad Jawaid, Yucheng Liu 

- You're a data scientist at a game development company  
- Your latest mobile game has two events you're interested in tracking: `buy a
  sword` & `join guild`
- Each has metadata characterstic of such events (i.e., sword type, guild name,
  etc)

## Tasks
- Instrument your API server to log events to Kafka
- 
- Assemble a data pipeline to catch these events: use Spark streaming to filter
  select event types from Kafka, land them into HDFS/parquet to make them
  available for analysis using Presto. 

- Use Apache Bench to generate test data for your pipeline.

- Produce an analytics report where you provide a description of your pipeline
  and some basic analysis of the events. Explaining the pipeline is key for this project!

- Submit your work as a git PR as usual. AFTER you have received feedback you have to merge 
  the branch yourself and answer to the feedback in a comment. Your grade will not be 
  complete unless this is done!

## Files 
- docker-compose.yml is docker compose file for setting up multiple services.
- game_api.py is the flask web application file.
- all the .json files are the pre-created metadata. 
- separate_events.py is the main python file for event processing, error handling and storing in parquet, making data accessible via Presto.
- term1_bash creates topics and run Flask
- term2_bash sets up to watch Kafka
- term3_bash creates events, send metadata to kafka, spark-submit

## Spin up the cluster
```
docker-compose up -d
```

## Check out Hadoop
```
docker-compose exec cloudera hadoop fs -ls /tmp/
```

## Change permissions of the three bash scripts to make them executable
```
chmod +x term1_bash
chmod +x term2_bash
chmod +x term3_bash
```

## Run bash script `term1_bash`: create topics and run Flask
```
./term1_bash
```

The `term1_bash` bash script creates topics: **swords, shields, potions, guilds, players, swords_md, shields_md, and potions_md**
Our design allows for the flexibility of load balancing. Each topic is independent so if one inventory item become more popular it will overwhelm other event processing.
It also starts **Flask**.

Wait 1 minute for flask to set up completely

## Run bash script `term2_bash`: set up to watch Kafka
Open new terminal window and leave it up. Run bash script `term2_bash`:
```
./term2_bash
```
or run:
```
docker-compose exec mids kafkacat -C -b kafka:29092 -t swords -o beginning
```
This sets up to watch kafka, it will run continuously.

## Run bash script `term3_bash`: create events, send metadata to kafka, spark-submit
Open new terminal and run bash script `term3_bash`:
```
./term3_bash
```
This bash script will send randomly generated events via workbench, sends player data (`Players.json`) and inventory metadata (`SwordsMeta.json`, `ShieldsMeta.json`, and `PotionsMeta.json`) to kafka, and runs spark-submit.

#### Error handling: no events in topic
What happens when the spark-submit job runs and there are no events? We check for it in `separate_events.py` and exit out if there are no events in a topic. Let's try it now while there are no events.
As an example, all inputs are validated to ensure that there are no empty events, with the exception of the default. 
```
if raw_swords.rdd.isEmpty():
     print("Exiting out because sword events are empty")
     exit()
```
Run command 
```
docker-compose exec spark spark-submit /w205/project-3-YusufLiu/separate_events.py
```

Output:
```
...
...
...
21/04/11 15:36:18 INFO DAGScheduler: Job 0 finished: runJob at PythonRDD.scala:446, took 1.270525 s
Exiting out because sword events are empty
21/04/11 15:36:18 INFO SparkContext: Invoking stop() from shutdown hook
21/04/11 15:36:18 INFO SparkUI: Stopped Spark web UI at http://172.19.0.6:4040
21/04/11 15:36:18 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
21/04/11 15:36:18 INFO MemoryStore: MemoryStore cleared
21/04/11 15:36:18 INFO BlockManager: BlockManager stopped
21/04/11 15:36:18 INFO BlockManagerMaster: BlockManagerMaster stopped
...
...
...
```

It works! We get the message: "Exiting out because sword events are empty"

### Events
First of all, we developed a python script to simulate the random user input events, and passed them through workbench by pasting them into the bash script 3.  
Example event:
```
docker-compose exec mids ab -n 1 -H "Host: player_001.att.com" http://localhost:5000/superman
```
- Host is the player id
- /superman is the purchase of the inventory item of superman potion. 

Please note we set the workbench node to n=1 to make discete events.

### Metadata
We are sending the meta data (detailed information regarding the players/inventory items) seperately.
The purpose of doing this is to setup/update the metadata without changing other parts of the pipeline. Additionally to reduce data redundacy, as events are joined with metadata in Presto. 

### spark-submit
Runs command 
```
docker-compose exec spark spark-submit /w205/project-3-YusufLiu/separate_events.py
```
separate_events.py is the main python file for event processing, error handling and storing in parquet, making data accessible via Presto.
## Check if landed in hadoop
```
docker-compose exec cloudera hadoop fs -ls /tmp/
```
Outputs:
```
drwxr-xr-x   - root   supergroup          0 2021-04-11 14:48 /tmp/guild_join_events
drwxrwxrwt   - mapred mapred              0 2016-04-06 02:26 /tmp/hadoop-yarn
drwx-wx-wx   - hive   supergroup          0 2021-04-11 14:46 /tmp/hive
drwxrwxrwt   - mapred hadoop              0 2016-04-06 02:28 /tmp/logs
drwxr-xr-x   - root   supergroup          0 2021-04-11 14:48 /tmp/players_events
drwxr-xr-x   - root   supergroup          0 2021-04-11 14:48 /tmp/potion_events
drwxr-xr-x   - root   supergroup          0 2021-04-11 14:48 /tmp/potions_md
drwxr-xr-x   - root   supergroup          0 2021-04-11 14:47 /tmp/raw_default_hits_data
drwxr-xr-x   - root   supergroup          0 2021-04-11 14:47 /tmp/raw_guild_data
drwxr-xr-x   - root   supergroup          0 2021-04-11 14:48 /tmp/raw_players_data
drwxr-xr-x   - root   supergroup          0 2021-04-11 14:47 /tmp/raw_potion_data
drwxr-xr-x   - root   supergroup          0 2021-04-11 14:48 /tmp/raw_potions_md_data
drwxr-xr-x   - root   supergroup          0 2021-04-11 14:47 /tmp/raw_shields_data
drwxr-xr-x   - root   supergroup          0 2021-04-11 14:48 /tmp/raw_shields_md_data
drwxr-xr-x   - root   supergroup          0 2021-04-11 14:47 /tmp/raw_sword_data
drwxr-xr-x   - root   supergroup          0 2021-04-11 14:48 /tmp/raw_swords_md_data
drwxr-xr-x   - root   supergroup          0 2021-04-11 14:48 /tmp/shield_events
drwxr-xr-x   - root   supergroup          0 2021-04-11 14:48 /tmp/shields_md
drwxr-xr-x   - root   supergroup          0 2021-04-11 14:48 /tmp/swords_events
drwxr-xr-x   - root   supergroup          0 2021-04-11 14:48 /tmp/swords_md
```
Any files prefixed with "raw_" are the events without processing (raw files not in tabular format).
All other files are in tabular format.


## Query with Presto to answer business questions and insights
### Start Presto
```
docker-compose exec presto presto --server presto:8080 --catalog hive --schema default
```

We create Views to join and consolidate data from multiple events and metadata tables. Additionally, it allows for more efficient data querying. 

### Create full Player table
```sql
Create view players_full as select a.player_id, a.player_name,a.player_location, b.event_type as guild_name from players_events a left join guild_join_events b on a.player_id = b.host;
```

### Join inventory events with meta data
#### Join `swords_events` with its meta data
```sql
Create view swords_price as select a.host, b.sword_name as item_name, b.price from swords_events a left join swords_md b on a.event_type = b.sword_name;
```
#### Join `potions_events` with its meta data
```sql
Create view potions_price as select a.host, b.potion_name as item_name, b.price from potion_events a left join potions_md b on a.event_type = b.potion_name;
```
#### Join `shields_events` with its meta data
```sql
Create view shields_price as select a.host, b.shield_name as item_name, b.price from shield_events a left join shields_md b on a.event_type = b.shield_name;
```

### Create table with prices and all purchases
```sql
Create view all_prices as select * from (select host, item_name, price from swords_price UNION ALL select host, item_name, price from shields_price UNION ALL select host, item_name, price from potions_price);
```

### Create Billings table with prices and player details
```sql
Create view billings as select a.*, b.item_name, b.price from players_full a left join all_prices b on a.player_id = b.host;
```

### Check that all 200 events have been stored
```sql
select count(*) as all_events from billings;
```
Output:
```
 all_events 
------------
        200 
```
- All **200 events** are accounted for!

### Total bill by player
```sql
select player_id, player_name, player_location, sum(cast(price as integer)) as total_bill from billings group by player_id, player_name, player_location;
```
Output:
```
     player_id      | player_name | player_location | total_bill 
--------------------+-------------+-----------------+------------
 player_001.att.com | Orange      | US              |       1995 
 player_002.att.com | Apple       | UK              |       1817 
 player_003.att.com | SwordMan    | Japan           |       1998 
 player_004.att.com | Devoid      | Australia       |       1903 
 player_005.att.com | Cookie      | US              |       1867 
 player_006.att.com | Zelda       | US              |       1396 
```
- total_bill column shows the total bill per player. Player SwordMan spent the most and player Zelda spent the least.

### Average purchase cost by player
```sql
select player_id, player_name, player_location, round(avg(cast(price as integer)),2) as average_purchase_cost from billings group by player_id, player_name, player_location;
```
Output:
```
     player_id      | player_name | player_location | average_purchase_cost 
--------------------+-------------+-----------------+-----------------------
 player_001.att.com | Orange      | US              |                  57.0 
 player_002.att.com | Apple       | UK              |                 55.06 
 player_003.att.com | SwordMan    | Japan           |                 51.23 
 player_004.att.com | Devoid      | Australia       |                 55.97 
 player_005.att.com | Cookie      | US              |                 58.34 
 player_006.att.com | Zelda       | US              |                  51.7 
```
- `average_purchase_cost` column shows the average cost per purchase by player.

### Total purchase cost by guild
```sql
select guild_name, sum(cast(price as integer)) as total_bill from billings group by guild_name;
```
Output:
```
   guild_name   | total_bill 
----------------+------------
 foreveryoung   |       5810 
 gonewiththewin |       5166 
```
- `total_bill` column shows the total bill by guild. The foreveryoung guild spent about 12% more than the gonewiththewin guild!

### Most/least popular sword
```sql
Create view swords_details as select a.host, b.*  from swords_events a left join swords_md b on a.event_type = b.sword_name;
```

#### Most popular sword
```sql
select inventory_id,price, sword_length, sword_name, sword_type, sword_weight,count(sword_name) as num_of_purchases from swords_details group by inventory_id,price, sword_length, sword_name, sword_type, sword_weight order by num_of_purchases desc LIMIT 1;
```
Output:
```
 inventory_id | price | sword_length | sword_name | sword_type | sword_weight | num_of_purchases 
--------------+-------+--------------+------------+------------+--------------+------------------
 SW002        | 60    | 50           | excalibur  | Diamond    | 6            |               25 
```
- The most popular sword purchase is **excalibur**.

#### Least popular sword
```sql
select inventory_id,price, sword_length, sword_name, sword_type, sword_weight,count(sword_name) as num_of_purchases from swords_details group by inventory_id,price, sword_length, sword_name, sword_type, sword_weight order by num_of_purchases asc LIMIT 1;
```
Output:
```
 inventory_id | price | sword_length | sword_name | sword_type | sword_weight | num_of_purchases 
--------------+-------+--------------+------------+------------+--------------+------------------
 SW003        | 35    | 35           | petrock    | Stone      | 4            |               10 
```
- The least popular sword purchase is **petrock**.

### Most/least popular shield
```sql
Create view shields_details as select a.host, b.* from shield_events a left join shields_md b on a.event_type = b.shield_name;
```

#### Most popular shield
```sql
select inventory_id,price, shield_category, shield_name, shield_weight,count(shield_name) as num_of_purchases from shields_details group by inventory_id,price, shield_category, shield_name, shield_weight order by num_of_purchases desc LIMIT 1;
```
Output:
```
 inventory_id | price | shield_category | shield_name | shield_weight | num_of_purchases 
--------------+-------+-----------------+-------------+---------------+------------------
 SH003        | 4     | Agile           | aegis       | 4             |               24 
```
- The most popular shield purchase is **aegis**.

#### Least popular shield
```sql
select inventory_id,price, shield_category, shield_name, shield_weight,count(shield_name) as num_of_purchases from shields_details group by inventory_id,price, shield_category, shield_name, shield_weight order by num_of_purchases asc LIMIT 1;
```
Output:
```
 inventory_id | price | shield_category | shield_name | shield_weight | num_of_purchases 
--------------+-------+-----------------+-------------+---------------+------------------
 SH001        | 80    | Strategic       | bubble      | 1             |               17 
```
- The least popular shield purchase is **bubble**.
