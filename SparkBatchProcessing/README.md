# SparkBatchProcessing

This is a short example of batch processing of real data using Apache Spark 2.0.

# Introduction and Requirements

We need to have installed Spark 2.0 and edit our .bashrc
  
     > cd /opt/
     > wget http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz 
     > tar -xvf spark-2.1.0-bin-hadoop2.7.tgz
     >cd /home/<my-user>/
     > sudo vim .bashrc

And we add the following lines:

     > export SPARK_HOME=/opt/spark-2.1.0-bin-hadoop2.7
     > export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python
     > export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip
     > export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python/pyspark

# Running

The project attached is made with Pycharm, and includes a real csv file inside "data" folder.
This csv file is downloaded from https://data.cityofchicago.org/Administration-Finance/Current-Employee-Names-Salaries-and-Position-Title/xzkq-xp2w and includes a list of more than 30k employees of the city of Chicago with the name, department, position and salary.

Runing the "batchAnalytics.py" file in our favourite IDE we get in console the average salary, the deparments with more employees and the top-3 highest salaries.

