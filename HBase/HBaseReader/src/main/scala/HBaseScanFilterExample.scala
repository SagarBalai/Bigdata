
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import java.io.PrintWriter
import java.io.File
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.BinaryComparator
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.RowFilter
import org.apache.hadoop.hbase.filter.PrefixFilter

object HBaseScanFilterExample {
  var LOGGER = LoggerFactory.getLogger(this.getClass)
  private val conf = new SparkConf().setAppName("Audit-History-task")
  private val sparkContext = new SparkContext(conf)
  private val hiveContext = new HiveContext(sparkContext)

  import hiveContext.implicits._

  case class HBaseRow(sNo: String, columnName: String, value: String, timeStamp: String)
  def main(args: Array[String]): Unit = {
    val sb = new StringBuilder
    val tableName = args(0)
    val columnFamily = args(1)
    val hiveAuditDB = args(2)
    val hiveAuditTable = args(3)
    val table = new HTable(HBaseConfiguration.create(), tableName)

    val prefixOfRowKey = "1491288220364"
    val valueForBinaryFilter = "sNo1"
    val scan = new Scan()
    scan.addFamily(columnFamily.getBytes)
    val filterList = new FilterList()
    filterList.addFilter(new PrefixFilter(Bytes.toBytes(prefixOfRowKey)))
    filterList.addFilter(new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(valueForBinaryFilter.getBytes())))
    scan.setFilter(filterList)
    val scanner = table.getScanner(scan)
    var result = scanner.next
    var rows = Seq[HBaseRow]()

    var count = 0;
    var totalColumn = 0
    while (result != null) {
      count += 1
      var rowkey = Bytes.toString(result.getRow).split("\\|")
      sb.append("\n\n")
      for (a <- rowkey)
        sb.append(a + ",")
      var familyMap = result.getFamilyMap(Bytes.toBytes(columnFamily))
      var itrr = familyMap.keySet.iterator
      while (itrr.hasNext()) {
        var key = itrr.next
        val rowKey = Bytes.toString(result.getRow)
        val compositeKey = rowKey.split("\\|")
        val serialNumber = compositeKey(1)
        val timeStamp = compositeKey(0)
        val columnName = Bytes.toString(key)
        val value = Bytes.toString(familyMap.get(key))
        rows :+= new HBaseRow(serialNumber, columnName, value, timeStamp)
        totalColumn += 1
        sb.append("\n" + Bytes.toString(key) + ":" + Bytes.toString(familyMap.get(key)) + ":")
      }
      result = scanner.next
      count += 1
    }

    LOGGER.warn("Hbase data is collected into DF, and count:" + count + ", total columns:" + totalColumn)
    val rdd = sparkContext.parallelize(rows, 1)
    val dataframe = rdd.toDF()
    dataframe.registerTempTable("temp1")
    hiveContext.sql("create table " + hiveAuditDB + "." + hiveAuditTable + " as select * from temp1")
    writeFile(sb.toString)
  }
  def mainWithFilterExample(args: Array[String]): Unit = {
    val sb = new StringBuilder
    val tableName = args(0)
    val columnFamily = args(1)
    val hiveAuditDB = args(2)
    val hiveAuditTable = args(3)

    LOGGER.warn("Audit Table Name" + tableName)
    LOGGER.warn("Audit column Family" + columnFamily)
    LOGGER.warn("Hive DB:" + hiveAuditDB)
    LOGGER.warn("Hive TableName:" + hiveAuditTable)

    var rows = Seq[HBaseRow]()

    val table = new HTable(HBaseConfiguration.create(), tableName)
    val scan = new Scan
    scan.addFamily(Bytes.toBytes(columnFamily))

    var colValFilter1 = new SingleColumnValueFilter(
      Bytes.toBytes(columnFamily), Bytes.toBytes("cse_sseorgname"),
      CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("Pepsi")));

    var colValFilter2 = new SingleColumnValueFilter(
      Bytes.toBytes(columnFamily), Bytes.toBytes("cse_serialnumber"),
      CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("JAD194602NS")));

    scan.setFilter(colValFilter1)
    scan.setFilter(colValFilter2)

    val scanner = table.getScanner(scan)
    var result = scanner.next()
    var count = 0
    var totalColumn = 0
    while (result != null) {
      var rowkey = Bytes.toString(result.getRow).split("\\|")
      sb.append("\n\n")
      for (a <- rowkey)
        sb.append(a + ",")
      var familyMap = result.getFamilyMap(Bytes.toBytes(columnFamily))
      var itrr = familyMap.keySet.iterator
      while (itrr.hasNext()) {
        var key = itrr.next
        val rowKey = Bytes.toString(result.getRow)
        val compositeKey = rowKey.split("\\|")
        val sNo = compositeKey(1)
        val timeStamp = compositeKey(0)
        val columnName = Bytes.toString(key)
        val value = Bytes.toString(familyMap.get(key))
        rows :+= new HBaseRow(sNo, columnName, value, timeStamp)
        totalColumn += 1
        sb.append("\n" + Bytes.toString(key) + ":" + Bytes.toString(familyMap.get(key)) + ":")
        //        LOGGER.warn("Key:" + Bytes.toString(key) + ",valaue:" + Bytes.toString(familyMap.get(key)) + "Row:" + Bytes.toString(result.getRow))
      }
      result = scanner.next
      count += 1
    }
    LOGGER.warn("Audit history collected from HBASE into DF, and count:" + count + ", total columns:" + totalColumn)
    val rdd = sparkContext.parallelize(rows, 1)
    val dataframe = rdd.toDF()
    dataframe.registerTempTable("ABS")
    LOGGER.warn("History will be persisted in ddHist.")
    hiveContext.sql("create table " + hiveAuditDB + "." + hiveAuditTable + " as select * from ABS")
    writeFile(sb.toString)
  }

  def main_working_withoutFilter(args: Array[String]): Unit = {
    val sb = new StringBuilder
    val tableName = args(0)
    val columnFamily = args(1)
    val hiveAuditDB = args(2)
    val hiveAuditTable = args(3)

    LOGGER.warn("Audit Table Name" + tableName)
    LOGGER.warn("Audit column Family" + columnFamily)
    LOGGER.warn("Hive DB:" + hiveAuditDB)
    LOGGER.warn("Hive TableName:" + hiveAuditTable)

    var rows = Seq[HBaseRow]()

    val table = new HTable(HBaseConfiguration.create(), tableName)
    val scan = new Scan
    scan.addFamily(Bytes.toBytes(columnFamily))
    val scanner = table.getScanner(scan)
    var result = scanner.next()
    var count = 0
    var totalColumn = 0
    while (result != null) {
      var rowkey = Bytes.toString(result.getRow).split("\\|")
      sb.append("\n\n")
      for (a <- rowkey)
        sb.append(a + ",")
      var familyMap = result.getFamilyMap(Bytes.toBytes(columnFamily))
      var itrr = familyMap.keySet.iterator
      while (itrr.hasNext()) {
        var key = itrr.next
        val rowKey = Bytes.toString(result.getRow)
        val compositeKey = rowKey.split("\\|")
        val serialNumber = compositeKey(1)
        val timeStamp = compositeKey(0)
        val columnName = Bytes.toString(key)
        val value = Bytes.toString(familyMap.get(key))
        rows :+= new HBaseRow(serialNumber, columnName, value, timeStamp)
        totalColumn += 1
        sb.append("\n" + Bytes.toString(key) + ":" + Bytes.toString(familyMap.get(key)) + ":")
      }
      result = scanner.next
      count += 1
    }
    LOGGER.warn("Audit history collected from HBASE into DF, and count:" + count + ", total columns:" + totalColumn)
    val rdd = sparkContext.parallelize(rows, 1)
    val dataframe = rdd.toDF()
    dataframe.registerTempTable("ABS")
    LOGGER.warn("History will be persisted in ddHist.")
    hiveContext.sql("create table " + hiveAuditDB + "." + hiveAuditTable + " as select * from ABS")
    writeFile(sb.toString)
  }

  def main1(args: Array[String]): Unit = {
    val tableName = "/app/hdpcst/hbasewarehouse/devicedata_audit"
    val columnFamily = "devicedata"

    newStuff(columnFamily, tableName)
    //    val table = new HTable(HBaseConfiguration.create(), tableName);
    //    val scan = new Scan();
    //    //    scan.setCaching(20);
    //
    //    scan.addFamily(Bytes.toBytes(columnFamily));
    //    val scanner = table.getScanner(scan);
    //
    //    //    for (result <-scanner.next(); (result != null); result <- scanner.next())
    //    var result = scanner.next
    //    while (result != null) {
    //      var keyValList = result.list
    //      for (i <- 0 to 5) {
    //        var keyValue = keyValList.get(i)
    //        LOGGER.warn("Qualifier : " + keyValue.getKeyString() + " : Value : " + Bytes.toString(keyValue.getValue()) + ",Timestamp:" + keyValue.getTimestamp + ",Row:" + Bytes.toString(keyValue.getRow))
    //        LOGGER.warn("Buffer:" + Bytes.toString(keyValue.getBuffer))
    //        LOGGER.warn("Key:" + Bytes.toString(keyValue.getKey))
    //        LOGGER.warn("Type:" + keyValue.getType)
    //      }
    //    }
  }

  //    def newStuffTrial(columnFamily: String, tableName: String) {
  //    val scan = new Scan();
  //    scan.addFamily(Bytes.toBytes(columnFamily));
  //    val table = new HTable(HBaseConfiguration.create(), tableName);
  //
  //    var resultScanner = table.getScanner(scan);
  //    var iterator = resultScanner.iterator();
  //    while (iterator.hasNext()) {
  //      var result = iterator.next();
  //      var familyItr = result.getFamilyMap(Bytes.toBytes(columnFamily)//  .entrySet().iterator()
  ////      while (familyItr.hasNext) {
  ////        val columnFamilyMap = familyItr.next()
  ////        var colKeyItr = columnFamilyMap.getValue.entrySet().iterator()
  ////        while (colKeyItr.hasNext) 
  ////        {
  ////          var entryVersion = colKeyItr.next
  ////          var entrySet = entryVersion.getValue.entrySet().iterator()
  ////          while (entrySet.hasNext()) 
  ////          {
  ////            var entry = entrySet.next
  ////            var row = Bytes.toString(result.getRow());
  ////            var column = Bytes.toString(entryVersion.getKey());
  ////            var value = Bytes.toString(entry.getValue())
  ////            var timesstamp = entry.getKey();
  ////            
  ////            LOGGER.warn("Column:"+column+",value:"+value+",row:"+row+",timeStamp:"+timesstamp)
  ////          }
  ////        }
  ////      }
  //    }
  //  }
  def newStuff(columnFamily: String, tableName: String) {
    val scan = new Scan();
    scan.addFamily(Bytes.toBytes(columnFamily));
    val table = new HTable(HBaseConfiguration.create(), tableName);

    var resultScanner = table.getScanner(scan);
    var iterator = resultScanner.iterator();
    while (iterator.hasNext()) {
      var result = iterator.next();
      var familyItr = result.getMap.entrySet().iterator()
      //      while (familyItr.hasNext) {
      //        val columnFamilyMap = familyItr.next()
      //        var colKeyItr = columnFamilyMap.getValue.entrySet().iterator()
      //        while (colKeyItr.hasNext) {
      //          var entryVersion = colKeyItr.next
      //          var entrySet = entryVersion.getValue.entrySet().iterator()
      //          while (entrySet.hasNext()) {
      //            var entry = entrySet.next
      //            var row = Bytes.toString(result.getRow());
      //            var column = Bytes.toString(entryVersion.getKey());
      //            var value = Bytes.toString(entry.getValue())
      //            var timesstamp = entry.getKey();
      //
      //            LOGGER.warn("Column:" + column + ",value:" + value + ",row:" + row + ",timeStamp:" + timesstamp)
      //          }
      //        }
      //      }
      var familyMap = result.getFamilyMap(Bytes.toBytes(columnFamily))
      var itrr = familyMap.keySet().iterator()
      while (itrr.hasNext()) {
        var key = itrr.next()
        LOGGER.warn("Key:" + Bytes.toString(key) + ",valaue:" + Bytes.toString(familyMap.get(key)) + "Row:" + Bytes.toString(result.getRow))
      }

    }

  }
  def writeFile(data: String) = {
    var file = new File("hbase-logging.txt")
    var printWriter = new PrintWriter(file);
    printWriter.write(data)
    printWriter.flush
    printWriter.close
  }

}