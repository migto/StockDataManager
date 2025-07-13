-- A股日线数据下载系统数据库初始化脚本
-- 创建时间: 2024
-- 数据库类型: SQLite
-- 注意: 所有日期字段使用DATE类型

-- 1. 股票基本信息表
-- 存储所有A股股票的基本信息
CREATE TABLE IF NOT EXISTS stocks (
    ts_code TEXT PRIMARY KEY,           -- 股票代码 (如: 000001.SZ)
    symbol TEXT NOT NULL,               -- 股票简称 (如: 000001)
    name TEXT NOT NULL,                 -- 股票名称 (如: 平安银行)
    area TEXT,                          -- 所在地区
    industry TEXT,                      -- 所属行业
    list_date DATE,                     -- 上市日期
    market TEXT,                        -- 市场类型 (主板/中小板/创业板)
    exchange TEXT,                      -- 交易所代码 (SSE/SZSE)
    curr_type TEXT,                     -- 交易货币
    list_status TEXT,                   -- 上市状态 (L上市/D退市/P暂停)
    delist_date DATE,                   -- 退市日期
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. 日线数据表
-- 存储股票的日线交易数据
CREATE TABLE IF NOT EXISTS daily_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_code TEXT NOT NULL,              -- 股票代码
    trade_date DATE NOT NULL,           -- 交易日期
    open REAL,                          -- 开盘价
    high REAL,                          -- 最高价
    low REAL,                           -- 最低价
    close REAL,                         -- 收盘价
    pre_close REAL,                     -- 前收盘价
    change REAL,                        -- 涨跌额
    pct_chg REAL,                       -- 涨跌幅 (%)
    vol REAL,                           -- 成交量 (手)
    amount REAL,                        -- 成交额 (千元)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ts_code, trade_date),        -- 联合唯一约束
    FOREIGN KEY (ts_code) REFERENCES stocks(ts_code)
);

-- 3. 下载状态跟踪表
-- 跟踪每只股票的下载状态和进度
CREATE TABLE IF NOT EXISTS download_status (
    ts_code TEXT PRIMARY KEY,           -- 股票代码
    last_download_date DATE,            -- 最后下载日期
    total_records INTEGER DEFAULT 0,    -- 总记录数
    status TEXT DEFAULT 'pending',      -- 状态 (pending/downloading/completed/error)
    error_message TEXT,                 -- 错误信息
    retry_count INTEGER DEFAULT 0,      -- 重试次数
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ts_code) REFERENCES stocks(ts_code)
);

-- 4. 系统配置表
-- 存储系统运行的各种配置参数
CREATE TABLE IF NOT EXISTS system_config (
    key TEXT PRIMARY KEY,               -- 配置键
    value TEXT,                         -- 配置值
    description TEXT,                   -- 配置说明
    data_type TEXT DEFAULT 'string',    -- 数据类型 (string/int/float/bool/date)
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. API调用记录表
-- 记录API调用历史，用于频率控制
CREATE TABLE IF NOT EXISTS api_call_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    api_name TEXT NOT NULL,             -- API名称 (stock_basic/daily/trade_cal等)
    call_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN DEFAULT 0,          -- 调用是否成功
    response_time INTEGER,              -- 响应时间 (毫秒)
    records_count INTEGER DEFAULT 0,    -- 返回记录数
    error_message TEXT,                 -- 错误信息
    request_params TEXT                 -- 请求参数 (JSON格式)
);

-- 6. 交易日历表
-- 存储交易日历信息，用于确定交易日
CREATE TABLE IF NOT EXISTS trade_calendar (
    cal_date DATE PRIMARY KEY,          -- 日期
    is_open BOOLEAN DEFAULT 0,          -- 是否开市
    pretrade_date DATE,                 -- 上一交易日
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
-- 日线数据表索引
CREATE INDEX IF NOT EXISTS idx_daily_data_ts_code ON daily_data(ts_code);
CREATE INDEX IF NOT EXISTS idx_daily_data_trade_date ON daily_data(trade_date);
CREATE INDEX IF NOT EXISTS idx_daily_data_ts_code_date ON daily_data(ts_code, trade_date);

-- API调用记录表索引
CREATE INDEX IF NOT EXISTS idx_api_call_log_api_name ON api_call_log(api_name);
CREATE INDEX IF NOT EXISTS idx_api_call_log_call_time ON api_call_log(call_time);
CREATE INDEX IF NOT EXISTS idx_api_call_log_success ON api_call_log(success);

-- 股票基本信息表索引
CREATE INDEX IF NOT EXISTS idx_stocks_symbol ON stocks(symbol);
CREATE INDEX IF NOT EXISTS idx_stocks_name ON stocks(name);
CREATE INDEX IF NOT EXISTS idx_stocks_industry ON stocks(industry);
CREATE INDEX IF NOT EXISTS idx_stocks_list_date ON stocks(list_date);

-- 下载状态表索引
CREATE INDEX IF NOT EXISTS idx_download_status_status ON download_status(status);
CREATE INDEX IF NOT EXISTS idx_download_status_last_download_date ON download_status(last_download_date);

-- 交易日历表索引
CREATE INDEX IF NOT EXISTS idx_trade_calendar_is_open ON trade_calendar(is_open);

-- 插入初始系统配置
INSERT OR REPLACE INTO system_config (key, value, description, data_type) VALUES
('tushare_token', '', 'Tushare Pro API Token', 'string'),
('db_version', '1.0.0', '数据库版本', 'string'),
('last_stock_info_update', '', '最后一次股票基本信息更新时间', 'datetime'),
('api_call_limit_per_minute', '60', '每分钟API调用限制', 'int'),
('api_call_limit_per_hour', '500', '每小时API调用限制', 'int'),
('api_call_limit_per_day', '3000', '每天API调用限制', 'int'),
('download_batch_size', '200', '批量下载大小', 'int'),
('retry_max_count', '3', '最大重试次数', 'int'),
('retry_delay_seconds', '1', '重试延迟秒数', 'int'),
('log_level', 'INFO', '日志级别', 'string'),
('auto_download_enabled', '1', '是否启用自动下载', 'bool'),
('download_start_time', '09:00', '下载开始时间', 'string'),
('download_end_time', '18:00', '下载结束时间', 'string');

-- 创建视图以便于查询
-- 股票基本信息视图（只显示上市股票）
CREATE VIEW IF NOT EXISTS v_active_stocks AS
SELECT ts_code, symbol, name, area, industry, list_date, market, exchange
FROM stocks
WHERE list_status = 'L' AND delist_date IS NULL;

-- 最新日线数据视图
CREATE VIEW IF NOT EXISTS v_latest_daily_data AS
SELECT d.ts_code, s.name, d.trade_date, d.close, d.pct_chg, d.vol, d.amount
FROM daily_data d
JOIN stocks s ON d.ts_code = s.ts_code
WHERE d.trade_date = (
    SELECT MAX(trade_date)
    FROM daily_data d2
    WHERE d2.ts_code = d.ts_code
);

-- 下载进度统计视图
CREATE VIEW IF NOT EXISTS v_download_progress AS
SELECT 
    COUNT(*) as total_stocks,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_stocks,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as error_stocks,
    SUM(CASE WHEN status = 'downloading' THEN 1 ELSE 0 END) as downloading_stocks,
    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_stocks,
    SUM(total_records) as total_records
FROM download_status;

-- 创建触发器以自动更新时间戳
-- 股票基本信息表更新触发器
CREATE TRIGGER IF NOT EXISTS update_stocks_timestamp 
AFTER UPDATE ON stocks
FOR EACH ROW
BEGIN
    UPDATE stocks SET updated_at = CURRENT_TIMESTAMP WHERE ts_code = NEW.ts_code;
END;

-- 下载状态表更新触发器
CREATE TRIGGER IF NOT EXISTS update_download_status_timestamp 
AFTER UPDATE ON download_status
FOR EACH ROW
BEGIN
    UPDATE download_status SET updated_at = CURRENT_TIMESTAMP WHERE ts_code = NEW.ts_code;
END;

-- 系统配置表更新触发器
CREATE TRIGGER IF NOT EXISTS update_system_config_timestamp 
AFTER UPDATE ON system_config
FOR EACH ROW
BEGIN
    UPDATE system_config SET updated_at = CURRENT_TIMESTAMP WHERE key = NEW.key;
END; 