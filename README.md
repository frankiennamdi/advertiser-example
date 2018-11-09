## IDE 
This application was developed with IDEA IntelliJ. It also has support for ScalaStyle.

## Overview 

The project uses coordinates in and around the city of Pittsburgh, Pennsylvania. This is the data I collected using 
Google Earth during my Master's GIS Class at Carnegie Mellon University, Pittsburgh. My data is measured
in kilometers, and I use 6371 as radius. There is also a meters version of the file.

## Design Consideration 

Considering that the location data is expected to be small (100 records) and to avoid the need for 
cross join or cartesian product we broadcast the set as a array of rows to all the executors. We 
then map each partition of the rows of devices evaluating each device in the partition against the 
broadcast variable. This result in a one time (devices x locations) loop a CPU bound operation. The 
alternative is to map each device resulting in devices x (device x locations). The latter runs the loop 
as many devices. 

## Data Files

### devices.parquet

This contains coordinates of apartment and libraries in and around Pittsburgh, Pennsylvania. 

### locations_of_interest_km.csv

This contains coordinates of shopping malls and complexes in and around Pittsburgh, Pennsylvania.

### Dates 

The dates are randomly selected from **2018-07-28T22:56:59.699Z**. And changing the **--target-day** command 
line option will select dates that a variable number of days before this date. 

## Schema 

The schemas are defined in the Schemas class. The output schema is shown below. 
 
 ```
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
  
```

### Code and Execution 

## Testing

### Framework 

The application uses Spec2 for testing.


## Build and Run Test
You can build it with the following question. This will also run the test.  

```./gradlew clean build```

## Run the code, you can override the default parameters

```
./gradlew runSpark -Ppargs='--devices-file=data/devices.parquet --ref-day=2018-07-28T22:56:59.699Z --location-file=data/locations_of_interest_km.csv --target-distance=50 --target-day=30'

```

or

```
./gradlew runSpark
```



  
