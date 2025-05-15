WITH departures AS (
        SELECT origin AS airport_code,
            COUNT(DISTINCT dest) AS nunique_to,
            COUNT(sched_dep_time) AS dep_planned,
            SUM(cancelled) AS dep_cancelled,
            SUM(diverted) AS dep_diverted,
            COUNT(arr_time) AS dep_n_flights
        FROM {{ref('prep_flights')}}
        GROUP BY origin
), 
arrivals AS (
        SELECT dest AS airport_code,
            COUNT(DISTINCT origin) AS nunique_from,
            COUNT(sched_arr_time) AS arr_planned,
            SUM(cancelled) AS arr_cancelled,
            SUM(diverted) AS arr_diverted,
            COUNT(arr_time) AS arr_n_flights
        FROM {{ref('prep_flights')}}
        GROUP BY dest
),
total_stats AS (
        SELECT d.airport_code,
                nunique_to, nunique_from, 
                (dep_planned + arr_planned) AS total_planned,
                (dep_cancelled + arr_cancelled) AS total_cancelled,
                (dep_diverted + arr_diverted) AS total_diverted,
                (dep_n_flights + arr_n_flights) AS total_flights
        FROM departures d
        JOIN arrivals a
        USING (airport_code)
)
SELECT ts.*, 
       a.city, 
       a.country, 
       a.name
FROM total_stats ts
JOIN {{ref('prep_airports')}} a
ON ts.airport_code = a.faa