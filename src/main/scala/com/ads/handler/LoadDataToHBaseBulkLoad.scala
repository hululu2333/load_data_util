package com.ads.handler

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Result, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HConstants, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object LoadDataToHBaseBulkLoad {

//  def main(args: Array[String]): Unit = {
//    if (args.length != 4)
//      throw new Exception("args must be 4:hive_table hbase_table hbase_rowkey loadtype")
//    val hiveTable = args(0)
//    val hbaseTable = args(1)
//    val hbaseRowkey = args(2)
//    val loadType = args(3)
//
//    val sparkConf = new SparkConf()
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))
//
//    val spark: SparkSession = SparkSession.builder()
//      .appName("hiveDataToHBaseBulkload")
//      .config(sparkConf)
//      .enableHiveSupport()
//      .getOrCreate()
//
//    //加载hBase在config.properties中的配置
//    val conditions = new LoadProperties().getProperties()
//    val bulkLoadPath = conditions.getProperty("hbase.bulkBaseDir") + hbaseTable
//
//    //加载hBase连接
//    val hadoopConf = new Configuration()
//    hadoopConf.set("fs.defaultFS", conditions.getProperty("hbase.defaultFS"))
//    val fileSystem = FileSystem.get(hadoopConf)
//    val hBaseConf = HBaseConfiguration.create(hadoopConf)
//    hBaseConf.set(HConstants.ZOOKEEPER_QUORUM, conditions.getProperty("hbase.zookeeper"))
//    hBaseConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTable)
//    val connection = ConnectionFactory.createConnection(hBaseConf)
//    val admin = connection.getAdmin
//
//    //删除hdfs中的bulkLoad目录
//    val start1 = System.currentTimeMillis()
//    dropHdfsDirIfExist(fileSystem, bulkLoadPath)
//    println(System.currentTimeMillis() - start1 + "ms:start1")
//
//    //创建hBase表
//    val start2 = System.currentTimeMillis()
//    createHTableIfNotExist(hbaseTable, conditions.getProperty("hbase.family"), loadType, admin)
//    println(System.currentTimeMillis() - start2 + "ms:start2")
//
//    //从hive中读取数据
//    val start3 = System.currentTimeMillis()
//    val hiveData = readDataFromHive(spark, hiveTable, loadType, hbaseRowkey)
//    println(System.currentTimeMillis() - start3 + "ms:start3")
//
//    //将数据格式转换成HBase的格式
//    val start4 = System.currentTimeMillis()
//    val data = transDataFromHiveToHBase(hiveData.getDataFrame, hiveData.getHiveCols, hbaseRowkey, conditions.getProperty("hbase.family"))
//    println(System.currentTimeMillis() - start4 + "ms:start4")
//    //将数据存储为hfile的形式存到hdfs
//    val start5 = System.currentTimeMillis()
//    val table = saveHFilesToHdfs(connection, hbaseTable, bulkLoadPath, hBaseConf, data)
//    println(System.currentTimeMillis() - start5 + "ms:start5")
//
//    //将hfile中的数据bulkload到hbase表中
//    val start6 = System.currentTimeMillis()
//    bulkLoadDataToHBase(hBaseConf, connection, hbaseTable, admin, table, bulkLoadPath)
//    println(System.currentTimeMillis() - start6 + "ms:start6")
//
//    //关闭连接
//    connection.close()
//    spark.close()
//  }
//
//  def bulkLoadDataToHBase(hbaseConf: Configuration, connection: Connection, hBaseTable: String, admin: Admin, table: Table, bulkLoadPath: String): Unit = {
//    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
//    val regionLocator = connection.getRegionLocator(TableName.valueOf(hBaseTable))
//    bulkLoader.doBulkLoad(new Path(bulkLoadPath), admin, table, regionLocator)
//  }
//
//  def saveHFilesToHdfs(connection: Connection, hbaseTable: String, bulkLoadPath: String, hbaseConf: Configuration, data: RDD[(ImmutableBytesWritable, KeyValue)]): Table = {
//    val table = connection.getTable(TableName.valueOf(hbaseTable))
//    val job = Job.getInstance(hbaseConf)
//    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
//    job.setMapOutputValueClass(classOf[KeyValue])
//    HFileOutputFormat2.configureIncrementalLoadMap(job, table)
//    data.saveAsNewAPIHadoopFile(
//      bulkLoadPath,
//      classOf[ImmutableBytesWritable],
//      classOf[KeyValue],
//      classOf[HFileOutputFormat2],
//      hbaseConf
//    )
//    table
//  }
//
//  def transDataFromHiveToHBase(df: RDD[Row], fields: Array[String], hbaseRowkey: String, family: String) = {
//    val data = df.map(row => {
//      val rowkey = row.getAs(hbaseRowkey).toString
//      var kvList: Seq[KeyValue] = List()
//      fields.foreach(field => {
//        var kv: KeyValue = null
//        if (row.getAs(field) != null) {
//          kv = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(family), Bytes.toBytes(field), Bytes.toBytes(row.getAs(field).toString))
//        } else {
//          kv = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(family), Bytes.toBytes(field), Bytes.toBytes(""))
//        }
//        kvList = kvList :+ kv
//      }
//      )
//      (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), kvList)
//    })
//
//    val hfileRDD: RDD[(ImmutableBytesWritable, KeyValue)] = data.flatMapValues(_.iterator)
//      .sortBy(x => (x._1, x._2.getKeyString), true)
//    hfileRDD
//  }
//
//  def readDataFromHive(spark: SparkSession, hiveTable: String, loadType: String, rowkey: String): HiveData = {
//    var sql: String = "select * from " + hiveTable + " where " + rowkey + " is not null and " + rowkey + " != '' "
//    if ("incr".equals(loadType)) {
//      sql = sql + " and insert_time > current_date()"
//    }
//    val hiveData = spark.sql(sql)
//    val fields = hiveData.columns
//    val df: RDD[Row] = hiveData.toDF().rdd
//    new HiveData(df, fields)
//  }
//
//  def createHTableIfNotExist(tableName: String, family: String, loadType: String, admin: Admin): Unit = {
//    val hBaseTableName = TableName.valueOf(tableName)
//    if (!admin.tableExists(hBaseTableName)) {
//      val tableDesc = new HTableDescriptor(hBaseTableName)
//      val hcd = new HColumnDescriptor(family)
//      hcd.setCompressionType(Compression.Algorithm.SNAPPY)
//      tableDesc.addFamily(hcd)
//      admin.createTable(tableDesc)
//    }
//  }
//
//  def dropHdfsDirIfExist(fileSystem: FileSystem, bulkLoadPath: String) = {
//    if (bulkLoadPath.contains("/tmp/bulkload")) {
//      if (fileSystem.exists(new Path(bulkLoadPath))) {
//        fileSystem.delete(new Path(bulkLoadPath), true)
//      }
//    } else {
//      throw new Exception("bulk Path must contains /tmp/bulkload/")
//    }
//  }
}
