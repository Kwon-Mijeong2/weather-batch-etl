SELECT

    collected_date,

    city,

    AVG(temperature) AS avg_temperature,

    MAX(temperature) AS max_temperature,

    MIN(temperature) AS min_temperature,

    AVG(humidity) AS avg_humidity

FROM {{ ref('stg_weather') }}

GROUP BY 1, 2