package com.ads.util

import java.io.FileInputStream
import java.util.Properties

object ProperUtils {
  val pro = new Properties()
  pro.load(new FileInputStream("target/classes/config.properties"))

  def getProperty(key: String): String = {
    pro.getProperty(key)
  }
}
