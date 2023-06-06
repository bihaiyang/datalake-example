CREATE TABLE source (
                         dim STRING,
                         user_id BIGINT,
                         price BIGINT,
                         row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),
                         WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '10',
      'fields.dim.length' = '1',
      'fields.user_id.min' = '1',
      'fields.user_id.max' = '100000',
      'fields.price.min' = '1',
      'fields.price.max' = '100000'
      );



CREATE TABLE sink (
                       dim STRING,
                       pv BIGINT,
                       sum_price BIGINT,
                       max_price BIGINT,
                       min_price BIGINT,
                       uv BIGINT,
                       window_start bigint
) WITH (
      'connector' = 'print'
      );

-- WITH source_with_total AS (
--     SELECT id , vc + 10 AS total
--     FROM source
--     WHERE id > 4
-- )
-- select id, sum (total)
-- from source_with_total
-- group by id;

-- 对于流计算，计算查询的结果所需的状态可能会无限增长。状态的大小取决于不同行数。可以设置状态的生存时间ttl 防止状态过大。
-- 但是可能会影响查询结果的正确性
-- select distinct id from source;


-- insert into sink
-- select dim,
--        count(*) AS pv,
--        sum(price) AS sum_price,
--        max(price) AS max_price,
--        min(price) AS min_price,
--        count(distinct user_id) AS uv,
--        cast((UNIX_TIMESTAMP(CAST(row_time AS STRING))) / 60 as bigint) AS window_start
-- from source
-- group by
--     dim,
--     cast((UNIX_TIMESTAMP(CAST(row_time AS STRING))) / 60 AS bigint);
-- UNIX_TIMESTAMP得到秒的时间戳，将秒级别时间戳 / 60 转化为 1min，

-- 供应商id、产品id、评级
SELECT
    supplier_id
     , rating
     , product_id
     , COUNT(*)
FROM (
         VALUES
         ('supplier1', 'product1', 4),
         ('supplier1', 'product2', 3),
         ('supplier2', 'product3', 3),
         ('supplier2', 'product4', 4)
     )
         AS Products(supplier_id, product_id, rating)
GROUP BY GROUPING SETS(
    (supplier_id, product_id, rating),
    (supplier_id, product_id),
    (supplier_id, rating),
    (supplier_id),
    (product_id, rating),
    (product_id),
    (rating),
    ()
    );

