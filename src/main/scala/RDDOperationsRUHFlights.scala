import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object RDDOperationsRUHFlights {

  def safeString(row: Row, colName: String): String = {
    Option(row.getAs[Any](colName)).map(_.toString).getOrElse("UNKNOWN")
  }

  def safeInt(row: Row, colName: String): Int = {
    Option(row.getAs[Any](colName)) match {
      case Some(v: Int)    => v
      case Some(v: Long)   => v.toInt
      case Some(v: Double) => v.toInt
      case Some(v)         => v.toString.toDouble.toInt
      case None            => 0
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RUH Flights RDD Operations")
      .master("local[*]")
      .getOrCreate()

    val df = DataPreprocessingRUHFlights.buildPreprocessedData(spark)
    val rdd = df.rdd

    println("========== ACTION 1: TOTAL PREPROCESSED ROWS ==========")
    println(s"Total rows in preprocessed dataset: ${rdd.count()}")

    // 1) Flights per hour of day
    val hourlyCounts = rdd
      .filter(row => !row.isNullAt(row.fieldIndex("hour_of_day")))
      .map(row => (safeInt(row, "hour_of_day"), 1))
      .reduceByKey(_ + _)
      .sortByKey()

    println("\n========== OPERATION 1: FLIGHTS BY HOUR OF DAY ==========")
    hourlyCounts.collect().foreach(println)

    println("\n========== ACTION 2: FIRST HOURLY RECORD ==========")
    val firstHourRecord = hourlyCounts.first()
    println(firstHourRecord)

    println("\n========== ACTION 3: TOTAL FLIGHTS (REDUCE) ==========")
    val totalFlightsFromHourly = hourlyCounts
      .map { case (_, count) => count }
      .reduce(_ + _)
    println(totalFlightsFromHourly)

    // 2) Weekend vs weekday traffic
    val weekendCounts = rdd
      .filter(row => !row.isNullAt(row.fieldIndex("is_weekend")))
      .map(row => (safeInt(row, "is_weekend"), 1))
      .reduceByKey(_ + _)

    println("\n========== OPERATION 2: WEEKEND VS WEEKDAY TRAFFIC ==========")
    weekendCounts.collect().foreach {
      case (1, count) => println(s"Weekend flights: $count")
      case (0, count) => println(s"Weekday flights: $count")
      case other      => println(other)
    }

    // 3) Peak vs non-peak traffic counts
    val peakCounts = rdd
      .filter(row => !row.isNullAt(row.fieldIndex("peak_traffic_label")))
      .map(row => (safeInt(row, "peak_traffic_label"), 1))
      .reduceByKey(_ + _)

    println("\n========== OPERATION 3: PEAK VS NON-PEAK COUNTS ==========")
    peakCounts.collect().foreach {
      case (1, count) => println(s"Peak flights: $count")
      case (0, count) => println(s"Non-peak flights: $count")
      case other      => println(other)
    }

    // 4) Top airlines by flight volume
    val airlineCounts = rdd
      .map(row => (safeString(row, "airline.name"), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    println("\n========== OPERATION 4: TOP 10 AIRLINES BY FLIGHT COUNT ==========")
    airlineCounts.take(10).foreach(println)

    // 5) Top destination airports by flight volume
    val destinationCounts = rdd
      .map(row => (safeString(row, "destination_airport_iata"), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    println("\n========== OPERATION 5: TOP 10 DESTINATIONS ==========")
    destinationCounts.take(10).foreach(println)

    // 6) Flight status distribution
    val statusCounts = rdd
      .map(row => (safeString(row, "status"), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    println("\n========== OPERATION 6: FLIGHT STATUS DISTRIBUTION ==========")
    statusCounts.collect().foreach(println)

    // 7) Peak traffic by airline
    val peakAirlineCounts = rdd
      .filter(row => !row.isNullAt(row.fieldIndex("peak_traffic_label")))
      .map(row => ((safeString(row, "airline.name"), safeInt(row, "peak_traffic_label")), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    println("\n========== OPERATION 7: AIRLINES DURING PEAK / NON-PEAK ==========")
    peakAirlineCounts.take(15).foreach(println)

    // 8) Peak traffic by destination
    val peakDestinationCounts = rdd
      .filter(row => !row.isNullAt(row.fieldIndex("peak_traffic_label")))
      .map(row => ((safeString(row, "destination_airport_iata"), safeInt(row, "peak_traffic_label")), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    println("\n========== OPERATION 8: DESTINATIONS DURING PEAK / NON-PEAK ==========")
    peakDestinationCounts.take(15).foreach(println)

    // 9) Distinct routes
    val distinctRoutes = rdd
      .map(row => (
        safeString(row, "origin_airport_iata"),
        safeString(row, "destination_airport_iata")
      ))
      .distinct()

    println("\n========== OPERATION 9: NUMBER OF DISTINCT ROUTES ==========")
    println(s"Distinct routes count: ${distinctRoutes.count()}")
    println("Sample routes:")
    distinctRoutes.take(10).foreach(println)

    // 10) Most frequent routes
    val routeCounts = rdd
      .map(row => (
        (safeString(row, "origin_airport_iata"), safeString(row, "destination_airport_iata")),
        1
      ))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    println("\n========== OPERATION 10: TOP 10 MOST FREQUENT ROUTES ==========")
    routeCounts.take(10).foreach(println)

    // 11) Cargo vs passenger traffic
    val cargoCounts = rdd
      .filter(row => !row.isNullAt(row.fieldIndex("isCargo_int")))
      .map(row => (safeInt(row, "isCargo_int"), 1))
      .reduceByKey(_ + _)

    println("\n========== OPERATION 11: CARGO VS PASSENGER FLIGHTS ==========")
    cargoCounts.collect().foreach {
      case (1, count) => println(s"Cargo flights: $count")
      case (0, count) => println(s"Passenger flights: $count")
      case other      => println(other)
    }

    // 12) Average hourly traffic by weekday/weekend
    val avgTrafficByWeekend = rdd
      .filter(row => !row.isNullAt(row.fieldIndex("is_weekend")) && !row.isNullAt(row.fieldIndex("hour_of_day")))
      .map(row => ((safeInt(row, "is_weekend"), safeInt(row, "hour_of_day")), 1))
      .reduceByKey(_ + _)
      .map { case ((isWeekend, hour), count) => (isWeekend, (count, 1)) }
      .reduceByKey { case ((sum1, n1), (sum2, n2)) => (sum1 + sum2, n1 + n2) }
      .mapValues { case (sum, n) => sum.toDouble / n }

    println("\n========== OPERATION 12: AVG FLIGHTS PER HOUR (WEEKDAY VS WEEKEND) ==========")
    avgTrafficByWeekend.collect().foreach {
      case (1, avg) => println(f"Weekend average hourly traffic: $avg%.2f")
      case (0, avg) => println(f"Weekday average hourly traffic: $avg%.2f")
      case other    => println(other)
    }

    spark.stop()
  }
}