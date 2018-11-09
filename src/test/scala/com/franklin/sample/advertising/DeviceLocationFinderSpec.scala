package com.franklin.sample.advertising

import java.time.{Instant, LocalDateTime, ZoneOffset}

import org.apache.log4j.{Level, Logger}
import org.specs2.mutable.SpecificationWithJUnit

/**
  * Created by frankie on 7/29/18.
  */
class DeviceLocationFinderSpec extends SpecificationWithJUnit with LocalSpark {
  Logger.getLogger("org").setLevel(Level.ERROR)

  import spark.implicits._

  "Find Devices with 50 kilometers of locations 30 days from 2018-07-28T22:56:59.699Z" in {

    val maxDistance = 50
    val maxDays = 30
    val referenceDateTime = LocalDateTime.ofInstant(Instant.parse("2018-07-28T22:56:59.699Z"), ZoneOffset.UTC)
    val deviceLocationFinder = new DeviceLocationFinder(spark)
    val deviceFinderResult = deviceLocationFinder.findDeviceAroundLocations("data/devices.parquet", "data/locations_of_interest_km.csv",
      referenceDateTime, maxDays, maxDistance)

    val maxResult = 250
    println(s"Max Results Displayed: $maxResult")
    deviceFinderResult.show(maxResult, truncate = false)
    deviceFinderResult.filter($"distance" > maxDistance).count() must_== 0
    deviceFinderResult.filter($"days" > maxDays).count() must_== 0
  }
}
