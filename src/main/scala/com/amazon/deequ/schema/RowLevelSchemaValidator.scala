/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.schema

import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.{col, expr, length, lit, not, regexp_extract, unix_timestamp, when,concat_ws}
import org.apache.spark.sql.types.{DataTypes, DecimalType, IntegerType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel


sealed trait ColumnDefinition {
  def name: String

  def isNullable: Boolean

  def castExpression(): Column = {
    col(name)
  }
}

private[this] case class StringColumnDefinition(
                                                 name: String,
                                                 isNullable: Boolean = true,
                                                 minLength: Option[Int] = None,
                                                 maxLength: Option[Int] = None,
                                                 matches: Option[String] = None)
  extends ColumnDefinition

private[this] case class IntColumnDefinition(
                                              name: String,
                                              isNullable: Boolean = true,
                                              minValue: Option[Int] = None,
                                              maxValue: Option[Int] = None)
  extends ColumnDefinition {

  override def castExpression(): Column = {
    col(name).cast(IntegerType).as(name)
  }
}

private[this] case class DecimalColumnDefinition(
                                                  name: String,
                                                  precision: Int,
                                                  scale: Int,
                                                  isNullable: Boolean = true)
  extends ColumnDefinition {

  override def castExpression(): Column = {
    col(name).cast(DecimalType(precision, scale)).as(name)
  }
}

private[this] case class TimestampColumnDefinition(
                                                    name: String,
                                                    mask: String,
                                                    isNullable: Boolean = true)
  extends ColumnDefinition {

  override def castExpression(): Column = {
    unix_timestamp(col(name), mask).cast(TimestampType).as(name)
  }
}


/** A simple schema definition for relational data in Andes */
case class RowLevelSchema(columnDefinitions: Seq[ColumnDefinition] = Seq.empty) {

  /**
   * Declare a textual column
   *
   * @param name       column name
   * @param isNullable are NULL values permitted?
   * @param minLength  minimum length of values
   * @param maxLength  maximum length of values
   * @param matches    regular expression which the column value must match
   * @return
   */
  def withStringColumn(
                        name: String,
                        isNullable: Boolean = true,
                        minLength: Option[Int] = None,
                        maxLength: Option[Int] = None,
                        matches: Option[String] = None)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ StringColumnDefinition(name, isNullable, minLength,
      maxLength, matches))
  }

  /**
   * Declare an integer column
   *
   * @param name       column name
   * @param isNullable are NULL values permitted?
   * @param minValue   minimum value
   * @param maxValue   maximum value
   * @return
   */
  def withIntColumn(
                     name: String,
                     isNullable: Boolean = true,
                     minValue: Option[Int] = None,
                     maxValue: Option[Int] = None)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ IntColumnDefinition(name, isNullable, minValue, maxValue))
  }

  /**
   * Declare a decimal column
   *
   * @param name       column name
   * @param precision  precision of values
   * @param scale      scale of values
   * @param isNullable are NULL values permitted?
   * @return
   */
  def withDecimalColumn(
                         name: String,
                         precision: Int,
                         scale: Int,
                         isNullable: Boolean = true)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ DecimalColumnDefinition(name, precision, scale, isNullable))
  }

  /**
   * Declare a timestamp column
   *
   * @param name       column name
   * @param mask       pattern for the timestamp
   * @param isNullable are NULL values permitted?
   * @return
   */
  def withTimestampColumn(
                           name: String,
                           mask: String,
                           isNullable: Boolean = true)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ TimestampColumnDefinition(name, mask, isNullable))
  }
}

/**
 * Result of enforcing a schema on textual data
 *
 * @param validRows      data frame holding the (casted) rows which conformed to the schema
 * @param numValidRows   number of rows which conformed to the schema
 * @param invalidRows    data frame holding the rows which did not conform to the schema
 * @param numInvalidRows number of rows which did not conform to the schema
 */
case class RowLevelSchemaValidationResult(
                                           validRows: DataFrame,
                                           numValidRows: Long,
                                           invalidRows: DataFrame,
                                           numInvalidRows: Long
                                         )

/** Enforce a schema on textual data */
object RowLevelSchemaValidator {

  private[this] def min_or_max(x: Any): Option[Int] =
  {
    if(x == null)
      return None
    else
      return Some(Integer.parseInt(x.toString))
  }

  def read_schema_from_data_frame(dataFrame: DataFrame): RowLevelSchema = {

    var columnDefination_list: List[ColumnDefinition] = List()


    dataFrame.collect().foreach(row =>{
      row(row.fieldIndex("type")) match {
        case "Integer" => {
          columnDefination_list = columnDefination_list ::: List(IntColumnDefinition(
            row(row.fieldIndex("colName")).toString,
            row(row.fieldIndex("isNullable")).asInstanceOf[Boolean],
            min_or_max(row(row.fieldIndex("minValue"))),
            min_or_max(row(row.fieldIndex("maxValue")))))
        }
        case "String" => {
          columnDefination_list = columnDefination_list ::: List(StringColumnDefinition(
            row(row.fieldIndex("colName")).toString,
            row(row.fieldIndex("isNullable")).asInstanceOf[Boolean],
            min_or_max(row(row.fieldIndex("minLength"))),
            min_or_max(row(row.fieldIndex("maxLength"))),
            Option[String](row(row.fieldIndex("matches")).asInstanceOf[String])
          ))
        }
        case "Decimal" => {
          columnDefination_list = columnDefination_list ::: List(DecimalColumnDefinition(
            name = row(row.fieldIndex("colName")).toString,
            precision = Integer.parseInt(row(row.fieldIndex("precision")).toString),
            scale = Integer.parseInt(row(row.fieldIndex("scale")).toString),
            isNullable = row(row.fieldIndex("isNullable")).asInstanceOf[Boolean]
          ))
        }
        case "TimeStamp" => {
          columnDefination_list = columnDefination_list ::: List(TimestampColumnDefinition(
            name = row(row.fieldIndex("colName")).toString,
            mask = row(row.fieldIndex("mask")).toString,
            isNullable = row(row.fieldIndex("isNullable")).asInstanceOf[Boolean]
          ))
        }
        case _ => {
          println("Invalid input schema")
        }
      }

    })

    RowLevelSchema(columnDefination_list)
  }


  private[this] val MATCHES_COLUMN = "__deequ__matches__schema"

  /**
   * Enforces a schema on textual data, filters out non-conforming columns and casts the result
   * to the requested types
   *
   * @param data                               a data frame holding the data to validate in string-typed columns
   * @param schema                             the schema to enforce
   * @param storageLevelForIntermediateResults the storage level for intermediate results
   *                                           (to control caching behavior)
   * @return results of schema enforcement
   */
  def validate(
                data: DataFrame,
                schema: RowLevelSchema,
                storageLevelForIntermediateResults: StorageLevel = StorageLevel.MEMORY_AND_DISK
              ): RowLevelSchemaValidationResult = {

    //    val dataWithMatches = data.withColumn(MATCHES_COLUMN, toCNF(schema))
    val dataWithMatches = toCNF(schema, data)
    dataWithMatches.persist(storageLevelForIntermediateResults)


    val validRows = extractAndCastValidRows(dataWithMatches, schema)
    val numValidRows = validRows.count()

    val invalidRows = dataWithMatches
      .where(not(col(MATCHES_COLUMN)))
    //      .drop(MATCHES_COLUMN) // Comment this so that it doesn't remove MATCHES_COLUMN

    val numInValidRows = invalidRows.count()

    dataWithMatches.unpersist(false)

    RowLevelSchemaValidationResult(validRows, numValidRows, invalidRows, numInValidRows)
  }

  private[this] def extractAndCastValidRows(
                                             dataWithMatches: DataFrame,
                                             schema: RowLevelSchema)
  : DataFrame = {

    val castExpressions = schema.columnDefinitions
      .map { colDef => colDef.name -> colDef.castExpression() }
      .toMap


    val projection = dataWithMatches.schema
      .map {
        _.name
      }
      //      .filter { _ != MATCHES_COLUMN } // Comment this so that it doesn't remove MATCHES_COLUMN
      .map { name => castExpressions.getOrElse(name, col(name)) }

    dataWithMatches.select(projection: _*).where(col(MATCHES_COLUMN))
  }

  private[this] def toCNF(schema: RowLevelSchema, data: DataFrame): DataFrame = {
    var changedData = data.withColumn("Condition Failed", lit(null))
    var nextCnf:Column = lit(true)
    schema.columnDefinitions.foldLeft(expr(true.toString)) { case (cnf, columnDefinition) =>

      nextCnf = cnf
      var errorMessage: Column = lit(null)

      if (!columnDefinition.isNullable) {
        nextCnf = nextCnf.and(col(columnDefinition.name).isNotNull)
        errorMessage = (col(columnDefinition.name).isNotNull)
      }

      val colIsNull = col(columnDefinition.name).isNull

      columnDefinition match {

        case intDef: IntColumnDefinition =>



          val colAsInt = col(intDef.name).cast(IntegerType)

          /* null or successfully casted */
          nextCnf = nextCnf.and(colIsNull.or(colAsInt.isNotNull))
          errorMessage = errorMessage.and(colIsNull.or(colAsInt.isNotNull))

          intDef.minValue.foreach { value =>
            nextCnf = nextCnf.and(colIsNull.isNull.or(colAsInt.geq(value)))
            errorMessage = errorMessage.and(colIsNull.isNull.or(colAsInt.geq(value)))
          }

          intDef.maxValue.foreach { value =>
            nextCnf = nextCnf.and(colIsNull.or(colAsInt.leq(value)))
            errorMessage = errorMessage.and(colIsNull.or(colAsInt.leq(value)))
          }

        case decDef: DecimalColumnDefinition =>

          val decType = DataTypes.createDecimalType(decDef.precision, decDef.scale)
          nextCnf = nextCnf.and(colIsNull.or(col(decDef.name).cast(decType).isNotNull))
          errorMessage = errorMessage.and(colIsNull.or(col(decDef.name).cast(decType).isNotNull))

        case strDef: StringColumnDefinition =>

          strDef.minLength.foreach { value =>
            nextCnf = nextCnf.and(colIsNull.or(length(col(strDef.name)).geq(value)))
            errorMessage = errorMessage.and(colIsNull.or(length(col(strDef.name)).geq(value)))

          }

          strDef.maxLength.foreach { value =>
            nextCnf = nextCnf.and(colIsNull.or(length(col(strDef.name)).leq(value)))
            errorMessage = errorMessage.and(colIsNull.or(length(col(strDef.name)).leq(value)))
          }

          strDef.matches.foreach { regex =>
            nextCnf = nextCnf.and(colIsNull.or(regexp_extract(col(strDef.name), regex, 0).notEqual("")))
            errorMessage = errorMessage.and(colIsNull.or(regexp_extract(col(strDef.name), regex, 0).notEqual("")))
          }

        case tsDef: TimestampColumnDefinition =>
          /* null or successfully casted */
          nextCnf = nextCnf.and(colIsNull.or(unix_timestamp(col(tsDef.name), tsDef.mask).cast(TimestampType).isNotNull))
          errorMessage = errorMessage.and(colIsNull.or(unix_timestamp(col(tsDef.name), tsDef.mask).cast(TimestampType).isNotNull))
      }
      changedData = changedData.withColumn(MATCHES_COLUMN, errorMessage)
      changedData = changedData.withColumn("Condition Failed", when(col(MATCHES_COLUMN) === false, concat_ws("",col("Condition Failed"),lit("---") ,lit(errorMessage.toString()))).otherwise(col("Condition Failed")))
      nextCnf

    }
    changedData.withColumn(MATCHES_COLUMN,nextCnf)
  }
}
