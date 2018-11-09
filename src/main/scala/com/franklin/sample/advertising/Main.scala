package com.franklin.sample.advertising

import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import org.apache.log4j.{Level, Logger}

object Main extends LocalSpark {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new Args()
    val jc = JCommander.newBuilder().addObject(args)
      .addObject(conf)
      .programName("DeviceLocationFinder")
      .build()

    jc.parse(args: _*)

    val deviceLocationFinder = new DeviceLocationFinder(spark)
    val deviceFinderResult = deviceLocationFinder.findDeviceAroundLocations(conf.devicesFile, conf.locationFile,
      LocalDateTime.ofInstant(Instant.parse(conf.referenceDateTime), ZoneOffset.UTC), conf.targetDays,
      conf.targetDistance)
    val maxResult = 250
    println(s"Max Results Displayed: $maxResult")
    deviceFinderResult.show(maxResult, truncate = false)
  }
}

@Parameters(separators = "=")
class Args {
  @Parameter(names = Array("--devices-file"))
  var devicesFile: String = "data/devices.parquet"

  @Parameter(names = Array("--location-file"))
  var locationFile: String = "data/locations_of_interest_km.csv"

  @Parameter(names = Array("--ref-day"))
  var referenceDateTime: String = "2018-07-28T22:56:59.699Z"

  @Parameter(names = Array("--target-distance"))
  var targetDistance: Double = 50

  @Parameter(names = Array("--target-day"))
  var targetDays: Int = 30
}
