package com.accolite

import org.apache.spark.sql.SparkSession

object spark_session {

  val spark = SparkSession
    .builder()
    .appName("hello-world")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")


  val titanicData = spark.read
    .option("header", "true")
    .option("dateFormat","MMMM d, yyyy") // Change date format
    .csv("test-data/titanic.csv")



}
