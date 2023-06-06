CREATE TABLE ws (
                    id INT,
                    vc INT,
                    pt AS PROCTIME(), --处理时间
                    et AS cast(CURRENT_TIMESTAMP as timestamp(3)), --事件时间
                    WATERMARK FOR et AS et - INTERVAL '5' SECOND   --watermark
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '10',
      'fields.id.min' = '1',
      'fields.id.max' = '3',
      'fields.vc.min' = '1',
      'fields.vc.max' = '100'
      );

--滚动窗口
select
    id,
    TUMBLE_START(et, INTERVAL '5' SECOND)  wstart,
    TUMBLE_END(et, INTERVAL '5' SECOND)  wend,
    sum(vc) sumVc
from ws
group by id, TUMBLE(et, INTERVAL '5' SECOND);


-- 滑动窗口
select
    id,
    HOP_START(pt, INTERVAL '3' SECOND,INTERVAL '5' SECOND)   wstart,
    HOP_END(pt, INTERVAL '3' SECOND,INTERVAL '5' SECOND)  wend,
    sum(vc) sumVc
from ws
group by id, HOP(et, INTERVAL '3' SECOND,INTERVAL '5' SECOND);


select
    id,
    SESSION_STRAT()