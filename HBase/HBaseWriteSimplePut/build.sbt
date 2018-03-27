name := "HbasePut"
version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.11" % "2.1.0",
  							"org.apache.spark" % "spark-sql_2.11" % "2.1.0",
  							"org.apache.spark" % "spark-hive_2.11" % "2.1.0",
  							"com.databricks" % "spark-avro_2.11" % "3.2.0",
  							"org.apache.hadoop" % "hadoop-core" % "0.20.2",
  							"org.apache.hbase" % "hbase-common" % "1.1.8",
  							"org.apache.hbase" % "hbase-client" % "1.1.8",
  							"org.apache.hbase" % "hbase-server" % "1.1.8"
  							
)