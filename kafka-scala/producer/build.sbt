name := "Kafka-Producer"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
							 "org.apache.kafka" % "kafka_2.10" % "0.9.0.1",
  							 "io.confluent" % "kafka-avro-serializer" % "2.0.0",
  						 	 "org.apache.avro" % "avro" % "1.8.1",
							 "org.apache.avro" % "avro-tools" % "1.8.1"
)
resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  "Confluent Maven Repo" at "http://packages.confluent.io/maven/"
)
