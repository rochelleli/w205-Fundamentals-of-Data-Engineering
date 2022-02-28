# Project 2: Tracking User Activity

In this project, you work at an ed tech firm. You've created a service that
delivers assessments, and now lots of different customers (e.g., Pearson) want
to publish their assessments on it. You need to get ready for data scientists
who work for these customers to run queries on the data. 

## Data

To get the data, run 
```
curl -L -o assessment-attempts-20180128-121051-nested.json https://goo.gl/ME6hjp`
```

# Tasks

Prepare the infrastructure to land the data in the form and structure it needs
to be to be queried.

- Publish and consume messages with Kafka
- Use Spark to transform the messages. 
- Use Spark to transform the messages so that you can land them in HDFS

Used Jupyter notebook instance with pyspark kernel.

## Files

- rochelleli-history.txt - history of  console 
- docker-compose.yml - docker file used
- project-2-rochelleli.ipynb - Jupyter notebook
- assessment-attempts-20180128-121051-nested.json - data file
