package com.spike.openlineage


import org.apache.spark.sql._

class SparkSessionConfig(appName: String, isLocal: Boolean) {

  private val sparkBuilder = SparkSession.builder().appName(appName)
  sparkBuilder.config("spark.sql.autoBroadcastJoinThreshold", -1)
  sparkBuilder.config("spark.sql.broadcastTimeout", "36000")
  sparkBuilder.config("spark.driver.maxResultSize", 0)
  sparkBuilder.config("spark.streaming.stopGracefullyOnShutdown", "true")

  if (isLocal) {
    sparkBuilder.master("local[*]")
  }

  private val spark: SparkSession = sparkBuilder.getOrCreate()
  spark.sparkContext.hadoopConfiguration.set("avro.mapred.ignore.inputs.without.extension", "false")
  spark.sqlContext.setConf("spark.sql.avro.compression.codec","snappy")

  def getSparkSession: SparkSession = {
    spark
  }

  def close: Unit = {
    spark.close()
  }

}

object SparkSessionConfig {

  def apply(appName: String, isLocal: Boolean): SparkSession =
    new SparkSessionConfig(appName, isLocal).getSparkSession
}

