insert into stock_3days_dwon
with step1 as (
    -- 第一步：标记每天是否满足「收盘价 < 开盘价」= 阴线
    select
        dt,
        code,
        stock_name,
        price_close,
        price_open,
        case when price_close < price_open then 1 else 0 end as is_down  -- 👈 这里改了
    from stock_detail
    where dt>='2026-03-02'
        and code not like '688%'
        and upper(stock_name) not like 'ST%'
),
step2 as (
    -- 第二步：用窗口函数生成行号，用于「连续日期分组」
    select
        *,
        row_number() over (partition by code order by dt) as rn,
        row_number() over (partition by code, is_down order by dt) as rn_down  -- 👈 这里改了
    from step1
),
step3 as (
    -- 第三步：筛选「连续3天及以上阴线」且「连续到最新日期」
    select
        code,
        max(stock_name) as stock_name,
        count(*) as number_of_consecutive_days,
        max(dt) as end_dt
    from step2
    where is_down = 1  -- 👈 这里改了：只看阴线
    group by code, rn - rn_down  -- 👈 这里改了
    having count(*) >= 3
       and max(dt) = '2026-03-26'  -- 最新交易日
),
final_result as (
    select
        s3.code,
        s3.stock_name,
        s3.number_of_consecutive_days,
        dst.industry,
        dst.industry_detail
    from step3 s3
    left join dim_stock_tag dst
        on s3.code = replace(replace(lower(dst.code), 'sz', ''), 'sh', '')
)
select * from final_result
order by number_of_consecutive_days desc;