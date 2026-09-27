-- Requêtes d'analyse à exécuter dans BigQuery une fois `dbt build` terminé.
-- Elles servent aussi de captures d'écran pour le README.

-- 1. Top 10 des zones de prise en charge par revenu
SELECT borough, zone_name, SUM(revenue) AS revenue, SUM(trips) AS trips
FROM `${GCP_PROJECT_ID}.nyc_taxi_analytics.agg_daily_zone`
GROUP BY 1, 2
ORDER BY revenue DESC
LIMIT 10;

-- 2. Profil horaire : semaine vs week-end
SELECT pickup_hour,
       SUM(IF(is_weekend, trips, 0)) AS weekend_trips,
       SUM(IF(is_weekend, 0, trips)) AS weekday_trips
FROM `${GCP_PROJECT_ID}.nyc_taxi_analytics.agg_hourly_demand`
GROUP BY 1
ORDER BY 1;

-- 3. Taux de pourboire moyen par mode de paiement
SELECT payment_type,
       COUNT(*) AS trips,
       ROUND(AVG(tip_rate), 4) AS avg_tip_rate
FROM `${GCP_PROJECT_ID}.nyc_taxi_analytics.fct_trips`
GROUP BY 1
ORDER BY trips DESC;

-- 4. Vitesse moyenne par heure (congestion)
SELECT pickup_hour, ROUND(AVG(avg_speed_mph), 2) AS avg_speed_mph
FROM `${GCP_PROJECT_ID}.nyc_taxi_analytics.fct_trips`
GROUP BY 1
ORDER BY 1;

-- 5. Répartition des trajets par longueur
SELECT trip_length_bucket, COUNT(*) AS trips, ROUND(AVG(total_amount), 2) AS avg_ticket
FROM `${GCP_PROJECT_ID}.nyc_taxi_analytics.fct_trips`
GROUP BY 1;
