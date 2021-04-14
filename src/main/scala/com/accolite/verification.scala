package com.accolite

import com.amazon.deequ.schema.{RowLevelSchema, RowLevelSchemaValidator}

object verification extends App {
//  var titanicData = spark_session.titanicData;
//
//  val schema = RowLevelSchema()
//    .withIntColumn("PassengerId", isNullable = false)
//    .withStringColumn("Name", isNullable = false)
//
//  val result = RowLevelSchemaValidator.validate(titanicData, schema)
  import spark_session.spark.implicits._
  val data = Seq(
    ("123", "Product A", "2012-07-22 22:59:59"),
    ("N/A", "Product B", null),
    ("456", null, "2012-07-22 22:59:59"),
    (null, "Product C", "2012-07-22 22:59:59")
  ).toDF("id", "name", "event_time")

  val schema = RowLevelSchema()
    .withIntColumn("id", isNullable = false)
    .withStringColumn("name", maxLength = Some(10))
    .withTimestampColumn("event_time", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false)

  val result = RowLevelSchemaValidator.validate(data, schema)

  print("Valid rows \n");
  result.validRows.show(false);
  print("Invalid rows \n");
  result.invalidRows.show(false);


  spark_session.spark.stop();
}

