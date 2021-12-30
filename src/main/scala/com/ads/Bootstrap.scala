package com.ads

import com.ads.handler.ToHbase

import scala.collection.mutable

/**
 * 目前的数据源只有hive，目标dbms可选
 * 新增目标dbms可以在handler中加对应的实现类
 */
object Bootstrap {
  def main(args: Array[String]): Unit = {
    val dbms = args(0)

    val param = new mutable.ArrayBuffer[String]
    for(i <- 1 until args.length){
      print(args(i) + " ")
      param += args(i)
    }

    dbms match {
      case "hbase" => new ToHbase().run(param)
      case _ => throw new Exception("wrong target dbms, eg: hbase")
    }
  }
}
