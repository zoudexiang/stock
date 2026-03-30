insert into stock_3days_up
with step1 as (
    -- 第一步：标记每天是否满足「收盘价 ≥ 开盘价」
    select
        dt,
        code,
        stock_name,
        price_close,
        price_open,
        case when price_close >= price_open then 1 else 0 end as is_up
    from stock_detail
    where dt>='2026-03-02'
        and code not like '688%'
        and upper(stock_name) not like 'ST'
),
step2 as (
    -- 第二步：用窗口函数生成行号，用于「连续日期分组」
    select
        *,
        row_number() over (partition by code order by dt) as rn,
        row_number() over (partition by code, is_up order by dt) as rn_up
    from step1
),
step3 as (
    -- 第三步：筛选「连续3天及以上」且「连续到最新日期2026-03-26」的股票
    select
        code,
        max(stock_name) as stock_name,  -- 取股票名称（假设名称固定，用聚合避免分组问题）
        count(*) as number_of_consecutive_days,
        max(dt) as end_dt
    from step2
    where is_up = 1  -- 只看满足条件的日期
    group by code, rn - rn_up  -- 核心：连续的满足条件的日期会有相同的「rn - rn_up」
    having count(*) >= 3  -- 连续3天及以上
       and max(dt) = '2026-03-26'  -- 关键：只保留「连续到最新日期」的股票
),
-- 第四步：关联行业维表，获取行业信息
final_result as (
    select
        s3.code,
        s3.stock_name,
        s3.number_of_consecutive_days,
        dst.industry,
        dst.industry_detail
    from step3 s3
    left join dim_stock_tag dst
        on s3.code = replace(replace(lower(dst.code), 'sz', ''), 'sh', '')  -- 用股票代码关联维表
)
-- 最终查询结果
select * from final_result
order by number_of_consecutive_days desc;