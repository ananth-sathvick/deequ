package com.accolite

import com.amazon.deequ.schema.{RowLevelSchema, RowLevelSchemaValidator}

object verification extends App {
  var titanicData = spark_session.titanicData;

  val schema = RowLevelSchema()
    .withIntColumn("PassengerId", isNullable = false)
    .withStringColumn("Name", isNullable = false)

  val result = RowLevelSchemaValidator.validate(titanicData, schema)

  print("Valid rows \n");
  result.validRows.show(false);
  print("Invalid rows \n");
  result.invalidRows.show(false);


  spark_session.spark.stop();
}

