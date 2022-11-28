package com.spike.openlineage

import org.apache.spark.sql.SparkSession

object ReadTextFile extends App{
  val filePath = "/Users/anujmehra/IdeaProjects/spark-stream-kafka-consumer/src/main/resources/sample-text-file.txt"

  implicit val spark: SparkSession = SparkSessionConfig("ReadATextFile", true)

  val df = spark.read.text(filePath)

  df.show(false)

}
