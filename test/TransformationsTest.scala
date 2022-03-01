// Databricks notebook source
// MAGIC %run "../src/Transformations"

// COMMAND ----------

import org.scalatest.FunSuite
import org.scalatest.exceptions.TestFailedException
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.scalatest._

class TestTransformation extends FunSuite with DataFrameComparer {

  test("with greeting test") {
    
    val inputDF = Seq(
      ("john"),
      ("tory")
    ).toDF("name")
    
    val expectedSchema = List(
      StructField("name", StringType, true),
      StructField("greeting", StringType, false)
    )

    val expectedData = Seq(
      Row("john", "hello world"),
      Row("tory", "hello world")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    val resultDF = withGreeting(inputDF)
    resultDF.show()
    
    assertSmallDataFrameEquality(resultDF, expectedDF)
  }  
}

// COMMAND ----------

durations.stats.run(new TestTransformation)
