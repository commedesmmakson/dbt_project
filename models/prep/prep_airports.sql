WITH airports_reorder AS (
    SELECT faa
    	   ,name
    	   ,lat
    	   ,lon
    	   ,alt
    	   ,tz
    	   ,dst
    	   ,country
    	   ,region
    	   ,city
    FROM {{ ref('staging_airports') }}
)
SELECT * FROM airports_reorder