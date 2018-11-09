package com.franklin.sample.advertising

import org.apache.spark.sql.SparkSession

/**
  * Spark Local Session Trait
  */
trait LocalSpark {
  lazy val spark: SparkSession = SparkSession.builder().appName("Advertising Prospect").master("local[*]").getOrCreate()
}
