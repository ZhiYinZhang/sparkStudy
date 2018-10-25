package com.entrobus.test

import java.io.{FileInputStream, InputStream}
import java.lang.NoSuchFieldException
import java.net.URL
import java.sql.{Connection, Timestamp}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import com.entrobus.generateData._
import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.io.FileUtils




object test {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    val path = Thread.currentThread().getContextClassLoader().getResource("test.properties").getPath()
    properties.load(new FileInputStream(path))

    println(properties.getProperty("kafkaHost"))
    println(properties.getProperty("topic"))
  }
}
