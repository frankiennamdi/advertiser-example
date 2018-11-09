package com.franklin.sample.advertising

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/*
  Class that finds the relationship between devices and point of interest
 */
class DeviceLocationFinder(spark: SparkSession) extends Serializable {

  def findDeviceAroundLocations(deviceLocationDataFilePath: String, locationsOfInterestDataFilePath: String,
                                referenceDateTime: LocalDateTime, numberOfDaysFromReferenceTime: Int,
                                distanceFromLocationsInMeters: Double): DataFrame = {

    println(s"Device File: $deviceLocationDataFilePath")
    println(s"Location File: $locationsOfInterestDataFilePath")
    println(s"Reference Day: $referenceDateTime")
    println(s"Num of Days: $numberOfDaysFromReferenceTime")
    println(s"Distance: $distanceFromLocationsInMeters")

    val devices = spark.read.option("header", "true").schema(Schemas.advertiserSchema)
      .parquet(deviceLocationDataFilePath)
    val locationsOfInterest = spark.read.option("header", "true").schema(Schemas.locationSchema)
      .csv(locationsOfInterestDataFilePath)

    val locationList = locationsOfInterest.distinct().collect()
    val locationListBroadCast = spark.sparkContext.broadcast(locationList)
    val distanceCalculator = new DefaultDistanceCalculator()

    def computeDateDiff(compareDateTime: Timestamp): Long = ChronoUnit.DAYS.between(LocalDateTime.ofInstant
    (compareDateTime.toInstant, ZoneOffset.UTC), referenceDateTime)

    val computeDateDiffUdf = udf((compareDateTime: Timestamp) => {
      computeDateDiff(compareDateTime)
    })

    def computeDistance(deviceRow: Row, locationRow: Row): Double = {
      distanceCalculator.calculateDistance(Location(deviceRow.getAs[Double]("latitude"),
        deviceRow.getAs[Double]("longitude")), Location(locationRow.getAs[Double]("Latitude"),
        locationRow.getAs[Double]("Longitude")), locationRow.getAs[Double]("Radius"))
    }

    val result = devices
      .filter(computeDateDiffUdf(devices.col("location_at")) <= numberOfDaysFromReferenceTime)
      .mapPartitions(entry => entry.map(device => {
        for {
          locations <- locationListBroadCast.value
          distance = computeDistance(device, locations)
          dateDiff = computeDateDiff(device.getAs[Timestamp]("location_at"))
          if distance <= distanceFromLocationsInMeters
        } yield (device.getAs[String]("advertiser_id"), device.getAs[Double]("latitude"),
          device.getAs[Double]("longitude"), locations.getAs[String]("Name"), locations.getAs[Double]("Latitude"),
          locations.getAs[Double]("Longitude"), locations.getAs[Double]("Radius"),
          device.getAs[LocalDateTime]("location_at"), distance, dateDiff)
      }).flatMap(d => d.map(e => Row(e.productIterator.toList: _*))))(RowEncoder(Schemas.resultSchema))

    result
  }
}
