CREATE KEYSPACE IF NOT EXISTS cryptopulse 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

USE cryptopulse;

CREATE TABLE real_time_aggregates (
    window_start timestamp, 
    window_end timestamp, 
    symbol text, 
    average_price double, 
    total_volume double, 
    high_price double,       
    low_price double,         
    price_swing_pct double,   
    is_anomaly boolean,       
    asset_name text, 
    category text, 
    PRIMARY KEY (window_start, symbol)
);