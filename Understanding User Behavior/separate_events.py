#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf

@udf('string')
def munge_event(event_as_json):
    event = json.loads(event_as_json)
    #event['Host'] = "moe"
    #event['Cache-Control'] = "no-cache"
    return json.dumps(event)


def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .enableHiveSupport() \
        .getOrCreate()
    

    raw_swords = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "swords") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    if raw_swords.rdd.isEmpty():
        print("Exiting out because sword events are empty")
        exit()
        
    munged_sword_events = raw_swords \
        .select(raw_swords.value.cast('string').alias('raw'),
                raw_swords.timestamp.cast('string')) \
        .withColumn('munged', munge_event('raw'))

    extracted_sword_events = munged_sword_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.munged))) \
        .toDF()
    #extracted_sword_events.show()
    
    sword_purchases = extracted_sword_events \
        .filter(extracted_sword_events.event_type != 'default')
    sword_purchases.show()

    # Write raw files to parquet (prior to extracting into tabular format)
    
    munged_sword_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/raw_sword_data")
  

    print('\n\n\n$$$$$$$$$$$$$$$$ Purchase Sword Events $$$$$$$$$$$$$$$\n\n\n')
        
    default_hits = extracted_sword_events \
        .filter(extracted_sword_events.event_type == 'default')
    default_hits.show()
    
    # Write raw files to parquet (prior to extracting into tabular format)
    default_hits \
            .write \
            .mode("overwrite") \
            .parquet("/tmp/raw_default_hits_data")
 
    
    print('\n\n\n$$$$$$$$$$$$$$$$ Default Sword Events $$$$$$$$$$$$$$$\n\n\n')
    
    raw_shields = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "shields") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    if raw_shields.rdd.isEmpty():
        print("Exiting out because shield events are empty")
        exit()

    munged_shield_events = raw_shields \
        .select(raw_shields.value.cast('string').alias('raw'),
                raw_shields.timestamp.cast('string')) \
        .withColumn('munged', munge_event('raw'))

    extracted_shield_events = munged_shield_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.munged))) \
        .toDF()
    extracted_shield_events.show()
    
    # Write raw files to parquet (prior to extracting into tabular format)

    munged_shield_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/raw_shields_data")


    print('\n\n\n$$$$$$$$$$$$$$$$ Purchase Shield Events $$$$$$$$$$$$$$$\n\n\n')
    
    raw_potions = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "potions") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    if raw_potions.rdd.isEmpty():
        print("Exiting out because potion events are empty")
        exit()
        
    munged_potion_events = raw_potions \
        .select(raw_potions.value.cast('string').alias('raw'),
                raw_potions.timestamp.cast('string')) \
        .withColumn('munged', munge_event('raw'))

    extracted_potion_events = munged_potion_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.munged))) \
        .toDF()
    extracted_potion_events.show()
    
    # Write raw files to parquet (prior to extracting into tabular format)
    munged_potion_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/raw_potion_data")


    print('\n\n\n$$$$$$$$$$$$$$$$ Purchase Potion Events $$$$$$$$$$$$$$$\n\n\n')
    
    raw_guilds = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "guilds") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    if raw_guilds.rdd.isEmpty():
        print("Exiting out because guild events are empty")
        exit()

    munged_guild_events = raw_guilds \
        .select(raw_guilds.value.cast('string').alias('raw'),
                raw_guilds.timestamp.cast('string')) \
        .withColumn('munged', munge_event('raw'))

    extracted_guild_events = munged_guild_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.munged))) \
        .toDF()
    #extracted_guild_events.show()
    
    # Write raw files to parquet (prior to extracting into tabular format)

    munged_guild_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/raw_guild_data")
    
    
    print('\n\n\n$$$$$$$$$$$$$$$$ All Guild Events $$$$$$$$$$$$$$$\n\n\n')
    
    guild_join = extracted_guild_events \
        .filter(extracted_guild_events.event_type != 'leave_guild')
    #guild_join.show()
    # guild_join \
        # .write \
        # .mode("overwrite") \
        # .parquet("/tmp/guild_join")

    print('\n\n\n$$$$$$$$$$$$$$$$ Join Guild Events $$$$$$$$$$$$$$$\n\n\n')
    
    
    raw_players = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe","players") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    if raw_players.rdd.isEmpty():
        print("Exiting out because player events are empty")
        exit()
    
    munged_players_events = raw_players \
        .select(raw_players.value.cast('string').alias('raw'),
                raw_players.timestamp.cast('string')) \
        .withColumn('munged', munge_event('raw'))

    extracted_players_events = munged_players_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.munged))) \
        .toDF()
    extracted_players_events.show()
    
    # Write raw files to parquet (prior to extracting into tabular format)

    munged_players_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/raw_players_data")

    
    print('\n\n\n$$$$$$$$$$$$$$$$ Player Table $$$$$$$$$$$$$$$\n\n\n')
    
    raw_swords_md = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe","swords_md") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    if raw_swords_md.rdd.isEmpty():
        print("Exiting out because sword metadata is empty")
        exit()    
    
    munged_swords_md_events = raw_swords_md \
        .select(raw_swords_md.value.cast('string').alias('raw'),
                raw_swords_md.timestamp.cast('string')) \
        .withColumn('munged', munge_event('raw'))

    extracted_swords_md_events = munged_swords_md_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.munged))) \
        .toDF()
    extracted_swords_md_events.show()
    
    # Write raw files to parquet (prior to extracting into tabular format)

    munged_swords_md_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/raw_swords_md_data")

    
    print('\n\n\n$$$$$$$$$$$$$$$$ Swords MD Table $$$$$$$$$$$$$$$\n\n\n')

    raw_shields_md = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe","shields_md") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    if raw_shields_md.rdd.isEmpty():
        print("Exiting out because shields metadata is empty")
        exit() 
        
    munged_shields_md_events = raw_shields_md \
        .select(raw_shields_md.value.cast('string').alias('raw'),
                raw_shields_md.timestamp.cast('string')) \
        .withColumn('munged', munge_event('raw'))

    extracted_shields_md_events = munged_shields_md_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.munged))) \
        .toDF()
    extracted_shields_md_events.show()
    
    # Write raw files to parquet (prior to extracting into tabular format)

    munged_shields_md_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/raw_shields_md_data")

    
    print('\n\n\n$$$$$$$$$$$$$$$$ Shields MD Table $$$$$$$$$$$$$$$\n\n\n')
    
    raw_potions_md = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe","potions_md") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    if raw_potions_md.rdd.isEmpty():
        print("Exiting out because potions metadata is empty")
        exit() 
    
    munged_potions_md_events = raw_potions_md \
        .select(raw_potions_md.value.cast('string').alias('raw'),
                raw_potions_md.timestamp.cast('string')) \
        .withColumn('munged', munge_event('raw'))

    extracted_potions_md_events = munged_potions_md_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.munged))) \
        .toDF()
    extracted_potions_md_events.show()
    
    # Write raw files to parquet (prior to extracting into tabular format)

    munged_potions_md_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/raw_potions_md_data")

    
    print('\n\n\n$$$$$$$$$$$$$$$$ Potions MD Table $$$$$$$$$$$$$$$\n\n\n')
    
    sword_purchases.registerTempTable("sword_purchases")
    
    spark.sql("""
        create external table swords_events
        stored as parquet
        location '/tmp/swords_events'
        as
        select * from sword_purchases
    """)

    extracted_shield_events.registerTempTable("extracted_shield_events")
    
    spark.sql("""
        create external table shield_events
        stored as parquet
        location '/tmp/shield_events'
        as
        select * from extracted_shield_events
    """)

    extracted_potion_events.registerTempTable("extracted_potion_events")
    
    spark.sql("""
        create external table potion_events
        stored as parquet
        location '/tmp/potion_events'
        as
        select * from extracted_potion_events
    """)
    
    guild_join.registerTempTable("guild_join")
    
    spark.sql("""
        create external table guild_join_events
        stored as parquet
        location '/tmp/guild_join_events'
        as
        select * from guild_join
    """)
    

    
    extracted_players_events.registerTempTable("extracted_players_events")
    
    spark.sql("""
        create external table players_events
        stored as parquet
        location '/tmp/players_events'
        as
        select * from extracted_players_events
    """)
   
    extracted_swords_md_events.registerTempTable(" extracted_swords_md_events")
    
    spark.sql("""
        create external table swords_md
        stored as parquet
        location '/tmp/swords_md'
        as
        select * from  extracted_swords_md_events
    """)

    extracted_shields_md_events.registerTempTable(" extracted_shields_md_events")
    
    spark.sql("""
        create external table shields_md
        stored as parquet
        location '/tmp/shields_md'
        as
        select * from  extracted_shields_md_events
    """)

    extracted_potions_md_events.registerTempTable(" extracted_potions_md_events")
    
    spark.sql("""
        create external table potions_md
        stored as parquet
        location '/tmp/potions_md'
        as
        select * from  extracted_potions_md_events
    """)
    
if __name__ == "__main__":
    main()
