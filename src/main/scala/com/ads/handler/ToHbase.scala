package com.ads.handler

import com.ads.param.HbaseParam
import org.apache.commons.codec.binary.Hex
import com.ads.util.ProperUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableOutputFormat}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer


class ToHbase {
  var spark: SparkSession = _
  var param: HbaseParam = _
  var queryLim: String = "1 = 1"
  val hdfsPath: String = ProperUtils.getProperty("hbase.bulkBaseDir")


  def run(args: Seq[String]): Unit = {
    prepare(args) //初始化成员变量
    load() //加载数据
  }


  def prepare(args: Seq[String]): Unit = {
//     创建spark session
    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))

    spark = SparkSession.builder()
      .appName("hiveDataToHBaseBulkload")
      .master("local[*]")
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

//    spark = SparkSession
//      .builder()
//      .appName("hiveDataToHBaseBulkload")
//      .enableHiveSupport()
//      .config("hive.exec.dynamic.partition", "true")
//      .config("hive.exec.dynamic.partition.mode", "nonstrict")
//      .getOrCreate()

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
  }


  def load(): Unit = {
    // 配置hadoop配置
//    val hadoopConf = new Configuration()
//    hadoopConf.set("fs.defaultFS", ProperUtils.getProperty("hbase.defaultFS"))
//    val hbaseConf = HBaseConfiguration.create(hadoopConf)
//    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, ProperUtils.getProperty("hbase.zookeeper"))
//    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, param.targetTable)

    // 创建测试用的表
    val spark2 = spark
    import spark2.implicits._
    val df1: DataFrame = Seq((1,"hu", 20211101)
                          , (2, "jia", 20211102)
                          , (3, "xiang", 20211101)).toDF("id", "name", "dt")
    df1.createOrReplaceTempView("sharehis")


    val df = sql(s"""
         |select concat(${param.rowKey}) rowkey, *
         |from ${param.sourceTable}
         |where ${queryLim}
         |"""
        .stripMargin)

    val colNames: Array[String] = df.columns.sortBy(x => x) // 拿到各个字段的字段名，并排序
//    colNames.foreach(println) //
    val cf = Bytes.toBytes("c") //列族
    val timestamp = System.currentTimeMillis() // 时间戳

    // 生成HFile并写到hdfs中
    val rdd: RDD[(ImmutableBytesWritable, KeyValue)] = df.rdd.flatMap(row => {
      val rk = row.getAs("rowkey").toString // 设置rowkey
      val rkb = Bytes.toBytes(rk)
      val rkw = new ImmutableBytesWritable(rkb)
      val arr = ArrayBuffer[(ImmutableBytesWritable, KeyValue)]()

      colNames.foreach { colName =>
        val colValue = row.getAs(colName).toString

        arr += ((rkw, new KeyValue(
          rkb,
          cf,
          Bytes.toBytes(colName),
          timestamp,
          Bytes.toBytes(colValue)
        )))
      }

      arr
    })

    rdd.foreach { case (rowkey, value) =>
      print("rowkey: " + rowkey)
      print("\t")
      println("value: " + value.toString)
    }
//      .saveAsNewAPIHadoopFile(
//        hdfsPath,
//        classOf[ImmutableBytesWritable],
//        classOf[KeyValue],
//        classOf[HFileOutputFormat2],
//        hbaseConf
//      )







//    rdd.flatMap { case (rk, columnFamily) =>
//      val arr = ArrayBuffer[(ImmutableBytesWritable, KeyValue)]()
//      val rkb = Hex.decodeHex(rk.toCharArray)
//      val rkw = new ImmutableBytesWritable(rkb)
//
//      columnFamily.toSeq.sortBy(_._1).foreach { case (colName, colValue) =>
//        arr += ((rkw, new KeyValue(
//          rkb,
//          cf,
//          Bytes.toBytes(colName),
//          timestamp,
//          Bytes.toBytes(colValue)
//        )))
//      }
//
//      totalAccumulator.add(1)
//      arr
//    }.saveAsNewAPIHadoopFile(
//      hdfsPath,
//      classOf[ImmutableBytesWritable],
//      classOf[KeyValue],
//      classOf[HFileOutputFormat2],
//      hbaseConf
//    )






    //    val targetTable = param.targetTable
    //
    //    //临时存放HFile的位置
    //    val bulkLoadPath = ProperUtils.getProperty("hbase.bulkBaseDir")+targetTable
    //
    //    //加载hBase连接
    //    val hadoopConf = new Configuration()
    //    hadoopConf.set("fs.defaultFS", ProperUtils.getProperty("hbase.defaultFS"))
    //
    //    val fileSystem = FileSystem.get(hadoopConf)
    //    val hBaseConf = HBaseConfiguration.create(hadoopConf)
    //    hBaseConf.set(HConstants.ZOOKEEPER_QUORUM,ProperUtils.getProperty("hbase.zookeeper"))
    //    hBaseConf.set(TableOutputFormat.OUTPUT_TABLE, targetTable)
    //
    //    val connection = ConnectionFactory.createConnection(hBaseConf)
    //    val admin = connection.getAdmin
    //
    //    //删除hdfs中的bulkLoad目录
    //    val start1 = System.currentTimeMillis()
    //    dropHdfsDirIfExist(fileSystem,bulkLoadPath)
    //    println(System.currentTimeMillis() - start1+"ms:start1")
    //
    //    //创建hBase表
    //    val start2 = System.currentTimeMillis()
    //    createHTableIfNotExist(targetTable,ProperUtils.getProperty("hbase.family"),loadType,admin)
    //    println(System.currentTimeMillis() - start2+"ms:start2")
    //
    //    //从hive中读取数据
    //    val start3 = System.currentTimeMillis()
    //    val hiveData = readDataFromHive(spark,hiveTable,loadType,hbaseRowkey)
    //    println(System.currentTimeMillis() - start3+"ms:start3")
    //    //将数据格式转换成HBase的格式
    //    val start4 = System.currentTimeMillis()
    //    val data = transDataFromHiveToHBase(hiveData.getDataFrame,hiveData.getHiveCols,hbaseRowkey,conditions.getProperty("hbase.family"))
    //    println(System.currentTimeMillis() - start4+"ms:start4")
    //    //将数据存储为hfile的形式存到hdfs
    //    val start5 = System.currentTimeMillis()
    //    val table = saveHFilesToHdfs(connection,hbaseTable,bulkLoadPath,hBaseConf,data)
    //    println(System.currentTimeMillis() - start5+"ms:start5")
    //    //将hfile中的数据bulkload到hbase表中
    //    val start6 = System.currentTimeMillis()
    //    bulkLoadDataToHBase(hBaseConf,connection,hbaseTable,admin,table,bulkLoadPath)
    //    println(System.currentTimeMillis() - start6+"ms:start6")
    //
    //    //关闭连接
    //    connection.close()
    //    spark.close()
  }

  def sql(query: String): DataFrame = {
    println(query)
    spark.sql(query)
  }
}
