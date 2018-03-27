name := "Hbase-scan"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.10" % "1.6.1",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1",
  "org.apache.spark" % "spark-hive_2.10" % "1.6.1",
  "org.apache.hadoop" % "hadoop-core" % "0.20.2",
  "com.databricks" % "spark-avro_2.10" % "2.0.1",
"org.apache.hbase" % "hbase-common" % "1.0.3",
"org.apache.hbase" % "hbase-client" % "1.0.3",
"org.apache.hbase" % "hbase-server" % "1.0.3"
)