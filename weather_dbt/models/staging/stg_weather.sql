SELECT

    city,

    temperature,

    humidity,

    weather,

    wind_speed,

    DATE(collected_at) AS collected_date,

    collected_at

FROM {{ source('weather_source', 'weather_raw') }}