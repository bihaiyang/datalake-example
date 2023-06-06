CREATE TABLE order_info (
                         uuid STRING,
                         city_id INT,
                         sex INT,
                         product_id INT,
                         order_time AS localtimestamp,
                         WATERMARK FOR order_time AS order_time
) WITH (
      'connector' = 'datagen',
      -- optional options --
      'rows-per-second'='5',
      'fields.uuid.kind'='sequence',
      'fields.uuid.start'='1',

      'fields.city_id.min'='0',
      'fields.city_id.max'='1',

      'fields.sex.min'='1',
      'fields.sex.max'='16',

      'fields.product_id.min'='1',
      'fields.product_id.max'='1000'
    );




CREATE TABLE ods_order_info (
                            uuid STRING,
                            city_id INT,
                            sex INT,
                            product_id INT,
                            order_time AS localtimestamp,
                            WATERMARK FOR order_time AS order_time
) WITH (
       'connector' = 'kafka',
       'topic' = 'ods_order_info',
       'properties.bootstrap.servers' = 'localhost:9092',
       'properties.group.id' = 'ods_order_info',
       'scan.startup.mode' = 'earliest-offset',
       'format' = 'JSON'
);
