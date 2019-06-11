import java.io.File

import java.io.FileOutputStream
import java.io.PrintWriter
import java.util.Properties

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import io.confluent.kafka.serializers.KafkaAvroSerializer

object KafkaAvroProducer {
  private var topic: String = "topic1";
  private var avroFilePath: String = "first.avro"
  private var brokerList: String = "hostname:portNumber" // eg  == "abc-001:9092"

  private val groupId = "group1"

  def main(args: Array[String]): Unit = {
    brokerList = args(0)
    topic = args(1)
    val schemaRegistryURL = args(2)
    avroFilePath = args(3)

    val props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    //Schema registry is confluent feature and required to for avro schema registration which will be used at time of consumption of data 
    //schemaRegistryURL == "http://host-name:8081/subjects/avro_schema/versions");
    props.put("schema.registry.url", schemaRegistryURL)

    // Other param can be used like batch size, linger.ms and all for tuning kafka as per use case
    // eg. if it is real time system with lacj 10 ms is allowed then you can keep linger.ms =2 ms so it will be 
    //definitely available within 2 ms to kafka consumer logic.

    val producer = new KafkaProducer[String, GenericRecord](props);

    val file = new File(avroFilePath)
    val datumReader = new GenericDatumReader[GenericRecord]()
    val dataFileReader = new DataFileReader(file, datumReader)

    val avroSchema = dataFileReader.getSchema

    var i = 1;
    while (dataFileReader.hasNext()) {
      val genericRecord = dataFileReader.next
      println("Sending record [" + i + "]")
      val record = new ProducerRecord[String, GenericRecord](topic, "", genericRecord)

      /* ProducerRecord needs 
      	1. topic name : topic for record
      	2. partition (Int): partition number to which want send record in cluster  --  optional
      	3. Key : Record key which will be used to find partition if 2 is missing   --  optional
      	4. Value : Actual Record 
       */

      val metadata = producer.send(record) // return type is Future and it is async when you call get then if result is not ready it will wait till the result 
      println("OffSet :" + metadata.get.offset())
      producer.flush
      i += 1
    }
  }
}