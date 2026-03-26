CREATE TABLE `dim_stock_tag` (
  `code` varchar(20) DEFAULT NULL COMMENT '股票代码',
  `industry` varchar(100) DEFAULT NULL COMMENT '所属行业',
  `industry_detail` varchar(100) DEFAULT NULL COMMENT '细分行业'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `section_detail` (
  `dt` varchar(10) DEFAULT NULL COMMENT '日期，格式 yyyy-MM-dd',
  `section_name` varchar(100) DEFAULT NULL COMMENT '版块名称',
  `rise` double DEFAULT NULL COMMENT '收盘涨幅',
  `rise_1min` double DEFAULT NULL COMMENT '1分钟涨速',
  `rise_4min` double DEFAULT NULL COMMENT '4分钟涨速',
  `main_force` double DEFAULT NULL COMMENT '主力净量',
  `main_force_amount` double DEFAULT NULL COMMENT '主力金额',
  `up_num` int DEFAULT NULL COMMENT '涨停数',
  `add_num` int DEFAULT NULL COMMENT '涨家数',
  `down_num` int DEFAULT NULL COMMENT '跌家数',
  `leader_stock` varchar(100) DEFAULT NULL COMMENT '领涨股',
  `rise_5day` double DEFAULT NULL COMMENT '5日涨幅',
  `rise_10day` double DEFAULT NULL COMMENT '10日涨幅',
  `rise_20day` double DEFAULT NULL COMMENT '20日涨幅',
  `concept_parse` varchar(1000) DEFAULT NULL COMMENT '概念解析',
  `create_date` varchar(100) DEFAULT NULL COMMENT '创建日期',
  `from_year` varchar(100) DEFAULT NULL COMMENT '年初至今',
  `from_20160127` varchar(100) DEFAULT NULL COMMENT '20160127至今',
  `ratio` double DEFAULT NULL COMMENT '量比',
  `trade` double DEFAULT NULL COMMENT '成交量(总手)',
  `trade_amount` double DEFAULT NULL COMMENT '成交额',
  `total_amount` double DEFAULT NULL COMMENT '总市值',
  `trading_market_capitalization` double DEFAULT NULL COMMENT '流通市值'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `stock_detail` (
  `dt` varchar(10) DEFAULT NULL COMMENT '日期，格式 yyyy-MM-dd',
  `code` varchar(6) DEFAULT NULL COMMENT '股票代码',
  `stock_name` varchar(100) DEFAULT NULL COMMENT '股票名称',
  `price_open` double DEFAULT NULL COMMENT '开盘价',
  `price_close` double DEFAULT NULL COMMENT '收盘价',
  `price_highest` double DEFAULT NULL COMMENT '最高价',
  `price_lowest` double DEFAULT NULL COMMENT '最低价',
  `trade` double DEFAULT NULL COMMENT '成交量(总手)',
  `trade_amount` double DEFAULT NULL COMMENT '成交额',
  `amplitude` double DEFAULT NULL COMMENT '振幅',
  `rise` double DEFAULT NULL COMMENT '收盘涨幅',
  `amount_increase_decrease` double DEFAULT NULL COMMENT '涨跌额',
  `turnover_rate` double DEFAULT NULL COMMENT '换手率'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;