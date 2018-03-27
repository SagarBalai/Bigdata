package com.cisco.cs.telemetry.persister.store

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory
import org.apache.spark.sql.Row
import java.util.ArrayList
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import com.cisco.cs.telemetry.persister.store.input.HBaseInput

/**
 * HBASE implementation and it will be used to persist data into HBASE.
 * @author sagar
 *
 */
class HBasePutExample {

  private val conf = HBaseConfiguration.create
  private val admin = new HBaseAdmin(conf)

  val LOGGER = LoggerFactory.getLogger(this.getClass)

  def store(dataFrame: Dataset[Row], tableName: String, columnFamily: String, rowKeyColumn: String) {
    if (!admin.tableExists(tableName)) {
      LOGGER.warn("Table " + tableName + " is not present in HBASE, creating new table " + tableName)

      val tableDesc = new HTableDescriptor(Bytes.toBytes(tableName))
      val columnFamilyDesc = new HColumnDescriptor(Bytes.toBytes(columnFamily))
      tableDesc.addFamily(columnFamilyDesc)
      admin.createTable(tableDesc)
    } else {
      LOGGER.warn("Table " + tableName + " is present in HBASE, incrementally pushing data into " + tableName + "/" + columnFamily)
    }

    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val mainTable = new HTable(conf, tableName);

    var putList: ArrayList[org.apache.hadoop.hbase.client.Row] = new ArrayList()
    var rowList = dataFrame.collectAsList
    var dtypes = dataFrame.dtypes
    LOGGER.warn(rowList.size + " devices processing started for device:" + columnFamily)
    val timeInMillis = System.currentTimeMillis

    for (rowIndex <- 0 to rowList.size - 1) {
      var deviceRow = rowList.get(rowIndex)
      var put: Put = null
      for (j <- 0 to dtypes.length - 1) {
        var dtype = dtypes(j)
        var columnName = dtype._1
        var columnValue = ""
        val rowKey = deviceRow.getAs[String](rowKeyColumn)
        if (rowKey == null) {
          throw new RuntimeException("Row key is null for key :" + rowKeyColumn)
        }
        put = new Put(rowKey.getBytes);
        if (deviceRow.get(j) != null) {
          columnValue = deviceRow.get(j).toString
          put.addColumn(columnFamily.getBytes, columnName.getBytes, columnValue.getBytes);
        }
      }
      putList.add(put)
    }
    val results = new Array[Object](putList.size)
    mainTable.batch(putList, results)
    LOGGER.warn(rowList.size + " data is processed and persisted and stored in " + tableName + "/" + columnFamily)

  }

}