package com.franklin.sample.advertising


import org.apache.spark.sql.types.{StructField, _}

/*
  Holds the Schemas for our data
 */
object Schemas {

  def advertiserSchema: StructType = {
    val schema = List(
      StructField("advertiser_id", StringType),
      StructField("latitude", DoubleType),
      StructField("longitude", DoubleType),
      StructField("location_at", TimestampType)
    )
    StructType(schema)
  }

  def locationSchema: StructType = {
    val schema = List(
      StructField("Name", StringType),
      StructField("Latitude", DoubleType),
      StructField("Longitude", DoubleType),
      StructField("Radius", DoubleType)
    )
    StructType(schema)
  }

  def resultSchema: StructType = {
    val schema = List(
      StructField("advertiser_id", StringType),
      StructField("advertiser_latitude", DoubleType),
      StructField("advertiser_longitude", DoubleType),
      StructField("location_name", StringType),
      StructField("location_latitude", DoubleType),
      StructField("location_longitude", DoubleType),
      StructField("location_radius", DoubleType),
      StructField("location_at", TimestampType),
      StructField("distance", DoubleType),
      StructField("days", LongType)
    )
    StructType(schema)
  }
}
