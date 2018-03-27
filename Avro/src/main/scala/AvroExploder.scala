import java.io.File
import java.io.PrintWriter

import scala.collection.mutable.StringBuilder

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord

/**
 * @author sbalai
 * 	Avro Exploder will be used to explode
 * 1. Schema
 * 2. Avro data
 */
object AvroExploder {

  def main(args: Array[String]): Unit = {
    var avroFilePath = "./first.avro"
    var avroFile = new File(avroFilePath)

    var datumReader = new GenericDatumReader[GenericRecord]
    var dataFileReader = new DataFileReader(avroFile, datumReader)
    var schema = dataFileReader.getSchema

    println("Schema\n\n" + schema + "\n")
    var str = new StringBuilder
    str.append("\n Schema \n\n " + schema + "\n\n Data : \n\n")
    while (dataFileReader.hasNext) {
      val record = dataFileReader.next
      str.append(record + "\n")
      println(record)
    }
    createFile("./first.txt", str.toString())

  }

  def createFile(fileName: String, data: String) = {
    var file = new File(fileName)
    var printWriter = new PrintWriter(file);
    printWriter.write(data)
    printWriter.flush()
    printWriter.close
  }

}