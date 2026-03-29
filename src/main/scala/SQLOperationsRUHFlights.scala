import org.apache.spark.sql.SparkSession

object SQLOperationsRUHFlights {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
          .appName("RUH Flights SQL Operations")
          .master("local[*]")
          .getOrCreate()

        // Build the preprocessed DataFrame from Phase 2/3
        val df = DataPreprocessingRUHFlights.buildPreprocessedData(spark)

        // Register temporary SQL view
        df.createOrReplaceTempView("ruh_flights")

        // =========================================================
        // QUERY 1: Total number of flights
        // =========================================================
        println("\n========== QUERY 1: TOTAL NUMBER OF FLIGHTS ==========")
        spark.sql("""
      SELECT COUNT(*) AS total_flights
      FROM ruh_flights
    """).show(false)

        // =========================================================
        // QUERY 2: Flights distribution by hour of day
        // =========================================================
        println("\n========== QUERY 2: FLIGHTS BY HOUR OF DAY ==========")
        spark.sql("""
      SELECT hour_of_day, COUNT(*) AS flight_count
      FROM ruh_flights
      WHERE hour_of_day IS NOT NULL
      GROUP BY hour_of_day
      ORDER BY hour_of_day
    """).show(24, false)

        // =========================================================
        // QUERY 3: Weekend vs weekday traffic
        // =========================================================
        println("\n========== QUERY 3: WEEKEND VS WEEKDAY TRAFFIC ==========")
        spark.sql("""
      SELECT
        CASE
          WHEN is_weekend = 1 THEN 'Weekend'
          WHEN is_weekend = 0 THEN 'Weekday'
          ELSE 'Unknown'
        END AS day_type,
        COUNT(*) AS flight_count
      FROM ruh_flights
      WHERE is_weekend IS NOT NULL
      GROUP BY is_weekend
      ORDER BY flight_count DESC
    """).show(false)

        // =========================================================
        // QUERY 4: Peak vs non-peak traffic
        // =========================================================
        println("\n========== QUERY 4: PEAK VS NON-PEAK TRAFFIC ==========")
        spark.sql("""
      SELECT
        CASE
          WHEN peak_traffic_label = 1 THEN 'Peak'
          WHEN peak_traffic_label = 0 THEN 'Non-Peak'
          ELSE 'Unknown'
        END AS traffic_type,
        COUNT(*) AS flight_count
      FROM ruh_flights
      WHERE peak_traffic_label IS NOT NULL
      GROUP BY peak_traffic_label
      ORDER BY flight_count DESC
    """).show(false)

        // =========================================================
        // QUERY 5: Top 10 airlines by flight volume
        // =========================================================
        println("\n========== QUERY 5: TOP 10 AIRLINES BY FLIGHT COUNT ==========")
        spark.sql("""
      SELECT
        COALESCE(`airline.name`, 'UNKNOWN') AS airline_name,
        COUNT(*) AS flight_count
      FROM ruh_flights
      GROUP BY `airline.name`
      ORDER BY flight_count DESC
      LIMIT 10
    """).show(false)

        // =========================================================
        // QUERY 6: Top 10 destination airports by flight volume
        // =========================================================
        println("\n========== QUERY 6: TOP 10 DESTINATIONS ==========")
        spark.sql("""
      SELECT
        COALESCE(destination_airport_iata, 'UNKNOWN') AS destination,
        COUNT(*) AS flight_count
      FROM ruh_flights
      GROUP BY destination_airport_iata
      ORDER BY flight_count DESC
      LIMIT 10
    """).show(false)

        // =========================================================
        // QUERY 7: Average hourly traffic (weekday vs weekend)
        // Uses CTE + aggregation
        // =========================================================
        println("\n========== QUERY 7: AVG FLIGHTS PER HOUR (WEEKDAY VS WEEKEND) ==========")
        spark.sql("""
      WITH hourly_counts AS (
        SELECT
          is_weekend,
          hour_of_day,
          COUNT(*) AS flights_in_hour
        FROM ruh_flights
        WHERE is_weekend IS NOT NULL
          AND hour_of_day IS NOT NULL
        GROUP BY is_weekend, hour_of_day
      )
      SELECT
        CASE
          WHEN is_weekend = 1 THEN 'Weekend'
          WHEN is_weekend = 0 THEN 'Weekday'
        END AS day_type,
        ROUND(AVG(flights_in_hour), 2) AS avg_hourly_traffic
      FROM hourly_counts
      GROUP BY is_weekend
      ORDER BY avg_hourly_traffic DESC
    """).show(false)

        // =========================================================
        // QUERY 8: Top 5 busiest destinations during peak periods
        // Uses window function
        // =========================================================
        println("\n========== QUERY 8: TOP DESTINATIONS DURING PEAK HOURS (WINDOW) ==========")
        spark.sql("""
      WITH destination_counts AS (
        SELECT
          COALESCE(destination_airport_iata, 'UNKNOWN') AS destination,
          COUNT(*) AS flight_count
        FROM ruh_flights
        WHERE peak_traffic_label = 1
        GROUP BY destination_airport_iata
      ),
      ranked_destinations AS (
        SELECT
          destination,
          flight_count,
          DENSE_RANK() OVER (ORDER BY flight_count DESC) AS rank_num
        FROM destination_counts
      )
      SELECT destination, flight_count, rank_num
      FROM ranked_destinations
      WHERE rank_num <= 5
      ORDER BY rank_num, destination
    """).show(false)

        spark.stop()
    }
}