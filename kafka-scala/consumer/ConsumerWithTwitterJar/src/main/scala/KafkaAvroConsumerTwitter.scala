import java.io.FileOutputStream
import java.io.PrintWriter

import scala.annotation.implicitNotFound

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

import com.twitter.bijection.avro.GenericAvroCodecs

import kafka.serializer.DefaultDecoder
import kafka.serializer.StringDecoder

/**
 * @author sagar
 * Consumer will read avro generic record with the help of Injection and decode byte of generic record 
 * to avro record.
 */
object KafkaAvroConsumerTwitter {

  private var zkQuorum = "host-name:2181"//sjc5-wadl-ta1:2181
  private var groupId = "group1"
  private var topics = "topic1"
  private val topicsMap = topics.split(",").map((_, 1)).toMap

  private val sparkConf = new SparkConf().setAppName("Kafka Consumer With Twitter")
  private val sc = new SparkContext(sparkConf)
  private val ssc = new StreamingContext(sc, Seconds(15))

  private val avro_schema = "{\"doc\":\"This will contain schema for each device\",\"fields\":[{\"doc\":\"The Type of Record.\",\"name\":\"recordType\",\"type\":\"string\"},{\"doc\":\"Version of the record.\",\"name\":\"recordVersion\",\"type\":\"string\"},{\"default\":0,\"doc\":\"Timestamp of the time of reporting.\",\"logicalType\":\"timestamp-millis\",\"name\":\"recordedAt\",\"type\":\"long\"},{\"name\":\"customerIDs\",\"type\":{\"doc\":\"This will contain customer ID Schema.\",\"fields\":[{\"default\":\"\",\"doc\":\"The SSE Org Name.\",\"name\":\"sseOrgName\",\"type\":\"string\"},{\"doc\":\"SSE Org ID.\",\"name\":\"sseOrgID\",\"type\":\"string\"},{\"default\":\"\",\"doc\":\"The Domain Name.\",\"name\":\"domainName\",\"type\":\"string\"},{\"default\":\"\",\"doc\":\"Virtual Account.\",\"name\":\"virtualAccount\",\"type\":\"string\"}],\"name\":\"customerID_record\",\"type\":\"record\"}},{\"name\":\"deviceInfo\",\"type\":{\"doc\":\"This will contain deviceInfo Schema.\",\"fields\":[{\"doc\":\"The SSE Device ID.\",\"name\":\"sseDeviceID\",\"type\":\"string\"},{\"doc\":\"deviceType.\",\"name\":\"deviceType\",\"type\":\"string\"},{\"doc\":\"The Device Name.\",\"name\":\"deviceName\",\"type\":\"string\"},{\"doc\":\"The Appliance UUID.\",\"name\":\"deviceProductId\",\"type\":\"string\"},{\"doc\":\"The Device Manager.\",\"name\":\"deviceManager\",\"type\":\"string\"},{\"doc\":\"The Platform Model.\",\"name\":\"platformModel\",\"type\":\"string\"},{\"doc\":\"The Serial Number.\",\"name\":\"serialNumber\",\"type\":\"string\"},{\"default\":0,\"doc\":\"System Uptime.\",\"logicalType\":\"timestamp-millis\",\"name\":\"systemUptime\",\"type\":\"long\"}],\"name\":\"deviceInfo_record\",\"type\":\"record\"}},{\"name\":\"licenseActivated\",\"type\":{\"doc\":\"This will contain License Activated Details.\",\"fields\":[{\"doc\":\"Items\",\"name\":\"items\",\"type\":{\"items\":{\"doc\":\"This will contain License Items.\",\"fields\":[{\"default\":\"\",\"doc\":\"The Type of License.\",\"name\":\"type\",\"type\":\"string\"},{\"default\":0,\"doc\":\"The Count for the license.\",\"name\":\"count\",\"type\":\"double\"}],\"name\":\"licenseItem_record\",\"type\":\"record\"},\"type\":\"array\"}}],\"name\":\"licenseActivated_schema\",\"type\":\"record\"}},{\"name\":\"versions\",\"type\":{\"doc\":\"This will contain Version Details.\",\"fields\":[{\"doc\":\"Items\",\"name\":\"items\",\"type\":{\"items\":{\"doc\":\"This will contain Version Items.\",\"fields\":[{\"default\":\"\",\"doc\":\"The Type of Version.\",\"name\":\"type\",\"type\":\"string\"},{\"default\":\"\",\"doc\":\"The Version.\",\"name\":\"version\",\"type\":\"string\"},{\"default\":0,\"doc\":\"The Last Updated.\",\"logicalType\":\"timestamp-millis\",\"name\":\"lastUpdated\",\"type\":\"long\"}],\"name\":\"versionItem_record\",\"type\":\"record\"},\"type\":\"array\"}}],\"name\":\"version_schema\",\"type\":\"record\"}},{\"name\":\"systemHealth\",\"type\":{\"doc\":\"This will contain System Health.\",\"fields\":[{\"name\":\"cpuUsage\",\"type\":{\"doc\":\"This will contain Items Schema.\",\"fields\":[{\"default\":\"\",\"doc\":\"Type.\",\"name\":\"type\",\"type\":\"string\"},{\"default\":0,\"doc\":\"The avg Start Time.\",\"logicalType\":\"timestamp-millis\",\"name\":\"avgStartTime\",\"type\":\"long\"},{\"default\":0,\"doc\":\"avg End Time.\",\"logicalType\":\"timestamp-millis\",\"name\":\"avgEndTime\",\"type\":\"long\"},{\"default\":0,\"doc\":\"avg Percent.\",\"name\":\"avgPercent\",\"type\":\"double\"},{\"default\":0,\"doc\":\"peak Start Time.\",\"logicalType\":\"timestamp-millis\",\"name\":\"peakStartTime\",\"type\":\"long\"},{\"default\":0,\"doc\":\"peak End Time.\",\"logicalType\":\"timestamp-millis\",\"name\":\"peakEndTime\",\"type\":\"long\"},{\"default\":0,\"doc\":\"peak Percent.\",\"name\":\"peakPercent\",\"type\":\"double\"}],\"name\":\"cpuUsageItems_schema\",\"type\":\"record\"}},{\"name\":\"memoryUsage\",\"type\":{\"doc\":\"This will contain Items Schema.\",\"fields\":[{\"default\":\"\",\"doc\":\"Type.\",\"name\":\"type\",\"type\":\"string\"},{\"default\":0,\"doc\":\"The avg Start Time.\",\"logicalType\":\"timestamp-millis\",\"name\":\"avgStartTime\",\"type\":\"long\"},{\"default\":0,\"doc\":\"avg End Time.\",\"logicalType\":\"timestamp-millis\",\"name\":\"avgEndTime\",\"type\":\"long\"},{\"default\":0,\"doc\":\"avg Percent.\",\"name\":\"avgPercent\",\"type\":\"double\"},{\"default\":0,\"doc\":\"peak Start Time.\",\"logicalType\":\"timestamp-millis\",\"name\":\"peakStartTime\",\"type\":\"long\"},{\"default\":0,\"doc\":\"peak End Time.\",\"logicalType\":\"timestamp-millis\",\"name\":\"peakEndTime\",\"type\":\"long\"},{\"default\":0,\"doc\":\"peak Percent.\",\"name\":\"peakPercent\",\"type\":\"double\"}],\"name\":\"memoryUsageItems_schema\",\"type\":\"record\"}},{\"name\":\"diskUsage\",\"type\":{\"doc\":\"This will contain disk Usage Schema.\",\"fields\":[{\"default\":0,\"doc\":\"The usedGb.\",\"name\":\"usedGb\",\"type\":\"double\"},{\"default\":0,\"doc\":\"freeGb.\",\"name\":\"freeGb\",\"type\":\"double\"},{\"default\":0,\"doc\":\"totalGb.\",\"name\":\"totalGb\",\"type\":\"double\"}],\"name\":\"diskUsage_schema\",\"type\":\"record\"}},{\"name\":\"thruputUsage\",\"type\":{\"doc\":\"This will contain Items Schema.\",\"fields\":[{\"default\":\"\",\"doc\":\"Type.\",\"name\":\"type\",\"type\":\"string\"},{\"default\":0,\"doc\":\"The avg Start Time.\",\"logicalType\":\"timestamp-millis\",\"name\":\"avgStartTime\",\"type\":\"long\"},{\"default\":0,\"doc\":\"avg End Time.\",\"logicalType\":\"timestamp-millis\",\"name\":\"avgEndTime\",\"type\":\"long\"},{\"default\":0,\"doc\":\"avg Bps.\",\"name\":\"avgBps\",\"type\":\"double\"},{\"default\":0,\"doc\":\"peak Start Time.\",\"logicalType\":\"timestamp-millis\",\"name\":\"peakStartTime\",\"type\":\"long\"},{\"default\":0,\"doc\":\"peak End Time.\",\"logicalType\":\"timestamp-millis\",\"name\":\"peakEndTime\",\"type\":\"long\"},{\"default\":0,\"doc\":\"peakBps.\",\"name\":\"peakBps\",\"type\":\"double\"}],\"name\":\"thruputItems_schema\",\"type\":\"record\"}},{\"name\":\"featureUsage\",\"type\":{\"doc\":\"This will contain Feature Usage\",\"fields\":[{\"name\":\"urlFiltering\",\"type\":{\"doc\":\"This will contain URL Filtering Schema.\",\"fields\":[{\"default\":0,\"doc\":\"The policyCount.\",\"name\":\"policyCount\",\"type\":\"double\"},{\"default\":0,\"doc\":\"blockedAllCount.\",\"name\":\"blockedAllCount\",\"type\":\"double\"}],\"name\":\"urlFiltering_schema\",\"type\":\"record\"}},{\"name\":\"malware\",\"type\":{\"doc\":\"This will contain Malware Schema.\",\"fields\":[{\"default\":0,\"doc\":\"The policyCount.\",\"name\":\"policyCount\",\"type\":\"double\"},{\"default\":0,\"doc\":\"scanned File Count.\",\"name\":\"scannedFileCount\",\"type\":\"double\"},{\"default\":0,\"doc\":\"fileCaptureCount.\",\"name\":\"malwareDetectedFileCount\",\"type\":\"double\"}],\"name\":\"malware_schema\",\"type\":\"record\"}},{\"name\":\"threat\",\"type\":{\"doc\":\"This will contain Threat Schema.\",\"fields\":[{\"default\":0,\"doc\":\"The policyCount.\",\"name\":\"policyCount\",\"type\":\"double\"},{\"default\":\"\",\"doc\":\"IPSPolicyNames.\",\"name\":\"IPSPolicyNames\",\"type\":\"string\"},{\"default\":0,\"doc\":\"blockedAllCount.\",\"name\":\"blockedAllCount\",\"type\":\"double\"}],\"name\":\"threat_schema\",\"type\":\"record\"}}],\"name\":\"featureUsage_Schema\",\"type\":\"record\"}}],\"name\":\"systemHealth_record\",\"type\":\"record\"}}],\"name\":\"device_record\",\"type\":\"record\"}"

  private val parser = new Schema.Parser();
  private val schema = parser.parse(avro_schema)
  private val recordInjection = GenericAvroCodecs.toBinary[GenericRecord](schema)

  private var isTwitter = false

  val sb = new StringBuilder

  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      zkQuorum = args(0); //eds-zk-dev-01:2181
      groupId = args(1)
      topics = args(2) 
      if (args(3).equalsIgnoreCase("true")) {
        isTwitter = true
      }
    }
    println("Zookeeper Quorem:" + zkQuorum)
    println("Group ID:" + groupId)
    println("Topic:" + topics)

    print("\nKafka Consumer starting")
    if (recordInjection == null) {
      println("Twitter object Null")
    }

    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum, "group.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "30000")

    if (isTwitter) {
      consumeWithTwitter(kafkaParams)
    } else {
      consumeWithoutTwitter(kafkaParams)
    }
    ssc.start()
    ssc.awaitTerminationOrTimeout(300000) //timeout: 60 seconds

  }

  def consumeWithoutTwitter(kafkaParams: Map[String, String]) {
    val lines = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicsMap).map(_._2)
    lines.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreach { x =>
          sb.append(x)
          println("Data :" + x)
        }
      } else {
        println("Empty partition RDD witout twitter")
      }
    })
  }
  def consumeWithTwitter(kafkaParams: Map[String, String]) {
    val kafkaStream = KafkaUtils.createStream[String, Array[Byte], DefaultDecoder, DefaultDecoder](ssc, kafkaParams, topicsMap, StorageLevel.MEMORY_ONLY_2).map(_._2)
    println("Kafka stream created.")

    var i = 1;
    kafkaStream.foreachRDD(rdd =>
      {
        if (!rdd.isEmpty) {

          rdd.foreach { avroRecord =>
            if (!avroRecord.isEmpty) {
              val genRecord = recordInjection.invert(avroRecord)
              print("\n Rec[" + i + "] :" + genRecord + "\t serialNumber :" + genRecord.get.get("deviceInfo.serialNumber"))
              sb.append("\n Rec[" + i + "] :" + genRecord)
              persistIntoFile(sb.toString)
            } else {
              println("Avro Record RDD is empty")
            }
          }

        } else {
          println("First RDD is empty ... this is partition RDD")
        }
      })

  }
  def persistIntoFile(data: String) {
    val filePath = "ConsumerRes.txt"
    var printWriter = new PrintWriter(new FileOutputStream(filePath, false))
    printWriter.write(data)
    printWriter.flush
    printWriter.close
  }
}
