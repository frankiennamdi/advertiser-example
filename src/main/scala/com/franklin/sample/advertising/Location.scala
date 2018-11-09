package com.franklin.sample.advertising

/*
  Represent a location
 */
case class Location(latitude: Double, longitude: Double)

trait DistanceCalculator extends Serializable{
  def calculateDistance(location1: Location, location2: Location, radius: Double): Double
}

/*
  Default Distance calculator
 */
class DefaultDistanceCalculator extends DistanceCalculator {

  override def calculateDistance(location1: Location, location2: Location, radius: Double): Double = {
    val dLat = Math.toRadians(location2.latitude - location1.latitude)
    val dLon = Math.toRadians(location2.longitude - location1.longitude)
    val lat1 = Math.toRadians(location1.latitude)
    val lat2 = Math.toRadians(location2.latitude)
    val a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.sin(dLon / 2) * Math.sin(dLon / 2) * Math.cos(lat1) * Math.cos(lat2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    val d = radius * c
    // round to 2 decimal places
    Math.round(d * 100.0)/100.0
  }
}
