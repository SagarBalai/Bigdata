Spark version : 2.x

Input : Dataframe
output : pushed into Hbase table- column family

read each row of dataframe and create put object and add into putList 
and at the end using batch operation on hbase table.