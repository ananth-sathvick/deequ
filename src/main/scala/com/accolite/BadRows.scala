package com.accolite

import com.amazon.deequ.schema._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{ SaveMode, SparkSession}

object BadRows {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession.builder()
      .master("local")
      .appName("Creating Spark Context with Spark Session")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      ("123", "Product A", "2012-07-22 22:59:59"),
      ("N/A", "Product B", null),
      ("456", null, "2012-22-22 22:59:59"),
      (null, "Product C", "2012-07-22 22:59:59")
    ).toDF("id", "name", "event_time")


    val dff = spark.read.option("multiline","true").json("src/main/resources/schema.json")


    val schema = RowLevelSchemaValidator.read_schema_from_data_frame(dff)


    val result = RowLevelSchemaValidator.validate(data, schema)


    println("Invalid rows")
    result.invalidRows.show();
  }
}
