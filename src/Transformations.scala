// Databricks notebook source
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

def withGreeting(df: DataFrame): DataFrame = {
    df.withColumn("greeting", lit("hello world"))
 }
