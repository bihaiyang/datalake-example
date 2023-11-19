CREATE TABLE taxi_ride (
     rideId bigint,
     taxiId bigint,
     driverId bigint,
     startTime timestamp,
     paymentType varchar,
     tip decimal,
     tolls decimal,
     totalFare decimal,
     proctime AS PROCTIME()
) WITH (
  'connector' = 'kafka', 
  'properties.bootstrap.servers' = 'localhost:9092', 
  'topic' = 'taxi_ride_1',
  'format' = 'json', 
  'scan.startup.mode' = 'earliest-offset',
  'properties.group.id' = 'taxi_ride_1'
);



CREATE TABLE taxi_ride_print (
        rideId bigint,
        taxiId bigint,
        driverId bigint,
        startTime timestamp,
        paymentType varchar,
        tip decimal,
        tolls decimal,
        totalFare decimal,
        proctime timestamp
)with (
        'connector' = 'print'
    );

insert into taxi_ride_print
select * from taxi_ride;




