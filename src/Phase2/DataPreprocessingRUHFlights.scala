import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.nio.file.{Files, Paths}

object DataPreprocessingRUHFlights {

  // --- Safe column accessor for names that contain dots like "movement.scheduledTime.utc"
  def c(name: String): Column = col(s"`$name`")

  private def ensureDir(path: String): Unit = {
    Files.createDirectories(Paths.get(path))
  }

  // --- 1) Missing summary (SAFE with dot column names)
  def missingSummary(df: DataFrame, label: String): DataFrame = {
    val cols = df.columns.toSeq

    val result =
      cols
        .map { name =>
          df.select(
            lit(name).alias("column"),
            sum(when(c(name).isNull, 1).otherwise(0)).cast("long").alias("missing_count")
          )
        }
        .reduce(_ union _)
        .filter(col("missing_count") > 0)
        .orderBy(desc("missing_count"))

    println(s"========== Missing Summary: $label ==========")
    result.show(200, truncate = false)

    result
  }

  // --- 2) Trim all string columns
  def withTrimmedStrings(df: DataFrame): DataFrame = {
    df.schema.fields.foldLeft(df) { (acc, f) =>
      f.dataType match {
        case StringType => acc.withColumn(f.name, trim(c(f.name)))
        case _          => acc
      }
    }
  }

  // --- 3) Normalize null-like strings to null (for string columns only)
  // IMPORTANT: we do NOT treat "Unknown" as null because it is a valid category in this dataset (e.g., status=Unknown).
  def normalizeNullLikeStrings(df: DataFrame): DataFrame = {
    val nullLike = Seq("null", "none", "na", "n/a", "nan", "")

    df.schema.fields.foldLeft(df) { (acc, f) =>
      f.dataType match {
        case StringType =>
          acc.withColumn(
            f.name,
            when(lower(c(f.name)).isin(nullLike: _*), lit(null)).otherwise(c(f.name))
          )
        case _ => acc
      }
    }
  }

  // --- 4) Uppercase codes safely (keeps nulls)
  def safeUpper(df: DataFrame, columnName: String): DataFrame = {
    if (df.columns.contains(columnName))
      df.withColumn(columnName, upper(c(columnName)))
    else df
  }

  // --- 5) Set invalid codes to null based on regex pattern
  def safeRegexNullify(df: DataFrame, columnName: String, pattern: String): (DataFrame, Long) = {
    if (!df.columns.contains(columnName)) return (df, 0L)

    val invalidCount =
      df.filter(c(columnName).isNotNull && !c(columnName).rlike(pattern)).count()

    val updated =
      df.withColumn(
        columnName,
        when(c(columnName).isNotNull && !c(columnName).rlike(pattern), lit(null)).otherwise(c(columnName))
      )

    (updated, invalidCount)
  }

  // --- 6) Robust timestamp parsing helpers
  private def parseTsMulti(colStr: Column): Column = {
    val normalized = regexp_replace(colStr, "Z$", "+00:00")

    coalesce(
      to_timestamp(normalized, "yyyy-MM-dd HH:mm:ssXXX"),
      to_timestamp(normalized, "yyyy-MM-dd HH:mmXXX"),
      to_timestamp(normalized, "yyyy-MM-dd HH:mm:ssX"),
      to_timestamp(normalized, "yyyy-MM-dd HH:mmX"),
      to_timestamp(normalized) // fallback
    )
  }

  // Create new parsed timestamp columns (do NOT overwrite originals)
  def addParsedTimestamps(df: DataFrame): DataFrame = {
    var out = df
    val localCol = "movement.scheduledTime.local"
    val utcCol   = "movement.scheduledTime.utc"

    if (df.columns.contains(localCol))
      out = out.withColumn("scheduled_local_ts", parseTsMulti(c(localCol)))

    if (df.columns.contains(utcCol))
      out = out.withColumn("scheduled_utc_ts", parseTsMulti(c(utcCol)))

    out
  }

  // Build peak label using hour_of_day counts (top percentile)
  def addPeakLabelByPercentile(df: DataFrame, percentile: Double = 0.90): (DataFrame, Double) = {
    val hourly =
      df.groupBy("hour_of_day")
        .agg(count(lit(1)).alias("flights_in_hour"))

    val q = hourly.stat.approxQuantile("flights_in_hour", Array(percentile), 0.0)
    val threshold = if (q.nonEmpty) q(0) else Double.PositiveInfinity

    val labeledHourly =
      hourly.withColumn(
        "peak_traffic_label",
        when(col("flights_in_hour") >= lit(threshold), lit(1)).otherwise(lit(0))
      )

    val out =
      df.join(labeledHourly.select("hour_of_day", "peak_traffic_label"), Seq("hour_of_day"), "left")
        .withColumn("peak_traffic_label", when(col("peak_traffic_label").isNull, 0).otherwise(col("peak_traffic_label")))

    (out, threshold)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RUH Flights Preprocessing")
      .master("local[*]")
      .getOrCreate()

    // Important for correct local feature extraction
    spark.conf.set("spark.sql.session.timeZone", "Asia/Riyadh")

    import spark.implicits._

    // ---------- PATHS (REPO-RELATIVE) ----------
    val inputPath = "data/raw/flights_RUH.parquet"

    // (Optional) output folders (created for consistency, but no writing is performed in this report-mode)
    val resultsPath = "results/phase2_preprocessing_stats"
    ensureDir("results")
    ensureDir(resultsPath)

    // ---------- LOAD ----------
    val raw = spark.read.parquet(inputPath)

    println("========== RAW DATASET ==========")
    raw.printSchema()

    val beforeRows = raw.count()
    val beforeCols = raw.columns.length
    println(s"Before rows: $beforeRows")
    println(s"Before cols: $beforeCols")

    val missingBefore = missingSummary(raw, "BEFORE")

    // ---------- CLEANING ----------
    var clean = raw.transform(withTrimmedStrings).transform(normalizeNullLikeStrings)

    // Uppercase important code columns
    val codeCols = Seq(
      "aircraft.reg",
      "aircraft.modeS",
      "airline.iata",
      "airline.icao",
      "origin_airport_iata",
      "origin_airport_icao",
      "destination_airport_iata",
      "destination_airport_icao"
    )
    codeCols.foreach(colName => clean = safeUpper(clean, colName))

    // Fix movement.terminal formatting (optional)
    if (clean.columns.contains("movement.terminal")) {
      clean = clean.withColumn("movement.terminal", upper(regexp_replace(c("movement.terminal"), "\\s+", "")))
    }

    // Parse timestamps into new columns
    clean = addParsedTimestamps(clean)

    // Filter rows missing local timestamp (your target derived from local time)
    if (clean.columns.contains("scheduled_local_ts")) {
      val beforeFilter = clean.count()
      clean = clean.filter(col("scheduled_local_ts").isNotNull)
      val afterFilter = clean.count()
      println(s"Filtered rows where scheduled_local_ts is null: removed ${beforeFilter - afterFilter}")
    }

    // Validate code formats (optional strict rules)
    var invalidStats = Seq.empty[(String, Long)]
    val formatRules = Seq(
      ("airline.iata", "^[A-Z0-9]{2}$"),
      ("airline.icao", "^[A-Z0-9]{3}$"),
      ("origin_airport_iata", "^[A-Z0-9]{3}$"),
      ("origin_airport_icao", "^[A-Z0-9]{4}$"),
      ("destination_airport_iata", "^[A-Z0-9]{3}$"),
      ("destination_airport_icao", "^[A-Z0-9]{4}$")
    )

    formatRules.foreach { case (colName, pattern) =>
      val (updated, invalidCount) = safeRegexNullify(clean, colName, pattern)
      clean = updated
      invalidStats = invalidStats :+ (colName, invalidCount)
    }

    println("========== Invalid Format Counts (set to null) ==========")
    invalidStats.foreach { case (colName, cnt) => println(s"$colName -> $cnt") }

    // ---------- FEATURE ENGINEERING (LOCAL time) ----------
    if (clean.columns.contains("scheduled_local_ts")) {
      clean = clean
        .withColumn("scheduled_date_local", to_date(col("scheduled_local_ts")))
        .withColumn("hour_of_day", hour(col("scheduled_local_ts")))
        .withColumn("day_of_week", dayofweek(col("scheduled_local_ts")))
        .withColumn("is_weekend", when(col("day_of_week").isin(6, 7), 1).otherwise(0)) // Fri+Sat
    }

    if (clean.columns.contains("isCargo")) {
      clean = clean.withColumn("isCargo_int", when(c("isCargo") === true, 1).otherwise(0))
    }

    // Fill some key text columns with UNKNOWN (optional)
    val fillUnknown = Seq("airline.icao", "airline.iata", "destination_airport_icao", "destination_airport_iata")
      .filter(clean.columns.contains)
    fillUnknown.foreach { colName =>
      clean = clean.withColumn(colName, when(c(colName).isNull, lit("UNKNOWN")).otherwise(c(colName)))
    }

    // ---------- PEAK LABEL ----------
    if (clean.columns.contains("hour_of_day")) {
      val (labeled, thr) = addPeakLabelByPercentile(clean, percentile = 0.90)
      clean = labeled
      println(s"========== Peak Label Threshold (Percentile 0.9) ==========")
      println(s"Hourly flights threshold = $thr")
    }

    // ---------- REPORT ----------
    val afterRows = clean.count()
    val afterCols = clean.columns.length

    println("========== AFTER CLEANING / PREPROCESSING ==========")
    println(s"After rows: $afterRows")
    println(s"After cols: $afterCols")
    println(s"Rows removed total: ${beforeRows - afterRows}")

    val missingAfter = missingSummary(clean, "AFTER")

    val missingCompare =
      missingBefore.withColumnRenamed("missing_count", "before_missing")
        .join(missingAfter.withColumnRenamed("missing_count", "after_missing"), Seq("column"), "full")
        .na.fill(0, Seq("before_missing", "after_missing"))
        .withColumn("before_missing", col("before_missing").cast("long"))
        .withColumn("after_missing", col("after_missing").cast("long"))
        .withColumn("missing_change", col("after_missing") - col("before_missing"))
        .orderBy(desc("before_missing"))

    println("========== Missing Values Before / After ==========")
    missingCompare.show(200, truncate = false)

    // Hourly counts + label distribution (for sanity)
    if (clean.columns.contains("hour_of_day") && clean.columns.contains("peak_traffic_label")) {
      println("========== Hourly Volume + Peak Label ==========")
      clean.groupBy("hour_of_day", "peak_traffic_label").count()
        .orderBy(col("hour_of_day"), col("peak_traffic_label"))
        .show(200, truncate = false)

      println("========== Peak Label Distribution ==========")
      clean.groupBy("peak_traffic_label").count().show(false)
    }

    // ---------- SNAPSHOT FOR REPORT (PRINT ONLY) ----------
    val cleanForSnapshot =
      if (clean.columns.contains("movement.quality"))
        clean.withColumn("movement.quality", concat_ws("|", c("movement.quality")))
      else clean

    println("========== FINAL PREPROCESSED DATASET SNAPSHOT (20 rows) ==========")
    cleanForSnapshot.limit(20).show(20, truncate = false)

    // NOTE: File writing is disabled in this report-mode to avoid Windows Hadoop/winutils configuration issues.

    spark.stop()
  }
}