It will read hbase rows with
1. all rows in table
2. Filtered rows using filter
3. Filtered using prefix filter which can be done on row key

Create dataframe using sparkContext.parallelize method

and add this dataframe into hive

This is done with the help of spark 1.6, if wanna do with 2.1 need to update some code
like adding SparkBuilder stuff which is starter point in spark 2.x
