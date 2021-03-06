package com.ads
import com.ads.handler.ToHbase

import scala.collection.mutable

/**
 * 目前的数据源只有hive，目标dbms可选
 * 新增目标dbms可以在handler中加对应的实现类
 * 入参：目标dbms等
 *
 * rowkey怎么设计？
 */
object Bootstrap {
  def main(args: Array[String]): Unit = {
    val dbms = args(0)

    val param = new mutable.ArrayBuffer[String]
    for(i <- 1 until args.length){
      println(args(i))
      param += args(i)
    }

    dbms match {
      case "hive" => new ToHbase().run(param)
      case _ => throw new Exception("wrong target dbms, eg: hive")
    }
  }
}
