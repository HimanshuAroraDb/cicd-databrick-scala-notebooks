// Databricks notebook source
// MAGIC %run "./Transformations"

// COMMAND ----------

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spark.implicits._

val inputDF = Seq(
      ("john"),
      ("tory")
    ).toDF("name")

val outputDF = withGreeting(inputDF)
outputDF.show
