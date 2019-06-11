name := "Kafka-twitter-Consumer"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
							"org.apache.spark" % "spark-core_2.10" % "2.1.0",
							"org.apache.spark" % "spark-sql_2.10" % "2.1.0",
  							"org.apache.kafka" % "kafka_2.10" % "0.9.0.1",
  							"com.twitter" % "bijection-core_2.10" % "0.9.0",
  							"com.twitter" % "bijection-avro_2.10" % "0.9.0",
  							"org.apache.spark" % "spark-streaming_2.10" % "2.1.0",					
  							"org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.3",
							"com.databricks" % "spark-avro_2.10" % "3.2.0" 							
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  "Confluent Maven Repo" at "http://packages.confluent.io/maven/"
)
