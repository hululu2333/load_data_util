package com.ads.handler

import com.ads.param.HbaseParam
import com.ads.util.ProperUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableOutputFormat}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HConstants, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

class ToHbase {
  var spark: SparkSession = _
  var param: HbaseParam = _
  var queryLim: String = "1 = 1"
  val hdfsPath: String = ProperUtils.getProperty("hbase.bulkBaseDir")
  val colFamily: String = ProperUtils.getProperty("hbase.family")
  var hbaseConf: Configuration = _
  var connection: Connection = _
  var admin: Admin = _
  var fileSystem: FileSystem = _


  def run(args: Seq[String]): Unit = {
    prepare(args) //初始化成员变量
    genHfile() //生成Hfile文件
    bulkLoad() //数据导入hbase
  }


  def prepare(args: Seq[String]): Unit = {
//     创建spark session
    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))

    spark = SparkSession.builder()
      .appName("hiveDataToHBaseBulkload")
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    // 解析选项和参数
    val hbaseParam = HbaseParam()
    val parser = new scopt.OptionParser[HbaseParam]("data_load") {
      head("option")
      opt[String]('s', "sourceTable")
        .text("source table")
        .action((x, c) => c.copy(sourceTable = x))
        .required()
      opt[String]('n', "nameSpace")
        .text("name space")
        .action((x, c) => c.copy(nameSpace = x))
        .required()
      opt[String]('t', "targetTable")
        .text("target table")
        .action((x, c) => c.copy(targetTable = x))
        .required()
      opt[String]('m', "mode")
        .text("load mode")
        .action((x, c) => c.copy(mode = x))
      opt[String]('i', "incrField")
        .text("increment field")
        .action((x, c) => c.copy(incrField = x))
      opt[String]('v', "incrValue")
        .text("increment value")
        .action((x, c) => c.copy(incrValue = x))
      opt[String]('r', "rowKey")
        .text("row key")
        .action((x, c) => c.copy(rowKey = x))
        .required()
    }

    parser.parse(args, hbaseParam) match {
      case Some(param) => {
        if ("incr".equals(param.mode)) {
          this.queryLim = s"${param.incrField} = '${param.incrValue}'" // 无论增量值是否是字符串型，这里用字符串型都是可以的
          if ("".equals(param.incrField) || "".equals(param.incrValue))
            throw new Exception("选择增量更新模式后，必须指定增量字段和增量值")
        } else if ("full".equals(param.mode)) {

        } else {
          throw new Exception("-m 选项后只能输入incr或full")
        }
        this.param = param
      }

      case None => throw new Exception("wrong arguments")
    }

    // 配置hbase与hadoop配置
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", ProperUtils.getProperty("hbase.defaultFS"))
    fileSystem = FileSystem.get(hadoopConf) // 用于删除hdfs文件夹
    hbaseConf = HBaseConfiguration.create(hadoopConf)
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, ProperUtils.getProperty("hbase.zookeeper"))
    println("zookeeper value from config：" + ProperUtils.getProperty("hbase.zookeeper")) // 临时打印
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, param.targetTable)
    connection = ConnectionFactory.createConnection(hbaseConf) // 这两行用于建立hbase表
    admin = connection.getAdmin
  }


  def genHfile(): Unit = {
//    // 创建测试用的表
//    val spark2 = spark
//    import spark2.implicits._
//    val df1: DataFrame = Seq((1, null, 20211101)
//      , (2, "jia", 20211102)
//      , (3, "xiang", 20211101)).toDF("id", "name", "dt")
//    df1.createOrReplaceTempView("sharehis")

    // 删除目标hdfs路径
    val bulkLoadPath = hdfsPath + param.targetTable
    if (fileSystem.exists(new Path(bulkLoadPath))) {
      fileSystem.delete(new Path(bulkLoadPath), true)
    }

    val df = sql(
      s"""
         |select concat(${param.rowKey}) rowkey, *
         |from ${param.sourceTable}
         |where ${queryLim}
         |"""
        .stripMargin)

    println("source data")
    df.show()

    val colNames: Array[String] = df.columns.sortBy(x => x) // 拿到各个字段的字段名，并排序
    val cf = Bytes.toBytes(colFamily) //列族
    val timestamp = System.currentTimeMillis() // 时间戳

    // 生成HFile并写到hdfs中
    df.rdd.flatMap(row => {
      val rk = row.getAs("" +
        "").toString // 设置rowkey
      val rkb = Bytes.toBytes(rk)
      val rkw = new ImmutableBytesWritable(rkb)
      val arr = ArrayBuffer[(ImmutableBytesWritable, KeyValue)]()

      colNames.foreach { colName =>
        var colValue: String = ""
        if (row.getAs(colName) != null) {
          colValue = row.getAs(colName).toString
        }

        arr += ((rkw, new KeyValue(
          rkb,
          cf,
          Bytes.toBytes(colName),
          timestamp,
          Bytes.toBytes(colValue)
        )))
      }

      arr
    }).saveAsNewAPIHadoopFile(
      bulkLoadPath,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hbaseConf
    )
  }


  def bulkLoad(): Unit = {
    // 创建hbase表
    val hBaseTableName = TableName.valueOf(param.targetTable)
    if (!admin.tableExists(hBaseTableName)) {
      val tableDesc = new HTableDescriptor(hBaseTableName)
      val hcd = new HColumnDescriptor(colFamily)
      hcd.setCompressionType(Compression.Algorithm.SNAPPY)
      tableDesc.addFamily(hcd)
      admin.createTable(tableDesc)
    }

    // 开始导入
    val bulkLoader: LoadIncrementalHFiles = new LoadIncrementalHFiles(hbaseConf)
    val regionLocator = connection.getRegionLocator(TableName.valueOf(param.targetTable))
    bulkLoader.doBulkLoad(new Path(hdfsPath), admin, connection.getTable(TableName.valueOf(param.targetTable)), regionLocator)
  }


  def sql(query: String): DataFrame = {
    println(query)
    spark.sql(query)
  }
}
