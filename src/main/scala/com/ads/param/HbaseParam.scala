package com.ads.param

/**
 * @param dbms 目标dbms
 * @param sourceTable 源表名
 * @param nameSpace hbase的命名空间
 * @param targetTable 目标表名
 * @param mode 加载模式
 * @param incrField 增量字段
 * @param incrValue 增量条件，例如：如果增量字段是日期的话，那可以是20211102
 * @param rowKey rowkey，可以由若干个原表中的字段组成
 */
case class HbaseParam(dbms : String = "hbase", sourceTable: String = "", nameSpace: String = "",
                     targetTable: String = "", mode: String = "full", incrField: String = "",
                      incrValue: String = "", rowKey: String = "")
