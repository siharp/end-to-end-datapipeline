CREATE TABLE IF NOT EXISTS geolocation
(
    geolocation_zip_code_prefix Int64,
    geolocation_lat             Float64,
    geolocation_lng             Float64,
    geolocation_city            String,
    geolocation_state           String
)
ENGINE = MergeTree()
ORDER BY geolocation_zip_code_prefix;