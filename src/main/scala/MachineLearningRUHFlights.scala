import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}

import java.nio.file.{Files, Paths}
import java.io.PrintWriter

object MachineLearningRUHFlights {

  def q(name: String): Column = col(s"`$name`")

  private def ensureDir(path: String): Unit = {
    Files.createDirectories(Paths.get(path))
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RUH Flights Machine Learning")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // =========================
    // PATHS
    // =========================
    val metricsOut    = "results/ml_metrics.txt"
    val modelOutPath  = "models/random_forest_pipeline"

    ensureDir("results")
    ensureDir("models")

    // =========================
    // LOAD DATA DIRECTLY FROM ORIGINAL FILE VIA PREPROCESSING
    // =========================
    var df = DataPreprocessingRUHFlights.buildPreprocessedData(spark)

    println("========== INPUT DATA FOR ML ==========")
    df.printSchema()
    println(s"Rows before ML prep: ${df.count()}")

    // =========================
    // RENAME DOTTED COLUMNS
    // =========================
    val renameMap: Seq[(String, String)] = Seq(
      "airline.name" -> "airline_name",
      "movement.terminal" -> "movement_terminal",
      "movement.airport.timeZone" -> "movement_airport_timeZone",
      "aircraft.model" -> "aircraft_model"
    )

    renameMap.foreach { case (oldName, newName) =>
      if (df.columns.contains(oldName)) {
        df = df.withColumn(newName, q(oldName)).drop(oldName)
      }
    }

    println("\n========== SCHEMA AFTER RENAMING ==========")
    df.printSchema()

    // =========================
    // ENSURE TYPES
    // =========================
    def castIfExists(df: DataFrame, colName: String, dt: DataType): DataFrame = {
      if (df.columns.contains(colName)) df.withColumn(colName, col(colName).cast(dt)) else df
    }

    df = castIfExists(df, "hour_of_day", IntegerType)
    df = castIfExists(df, "day_of_week", IntegerType)
    df = castIfExists(df, "is_weekend", IntegerType)
    df = castIfExists(df, "isCargo_int", IntegerType)
    df = castIfExists(df, "peak_traffic_label", IntegerType)

    // Label column for Spark ML
    df = df.withColumn("label", col("peak_traffic_label").cast(DoubleType))

    // Drop rows with null label just in case
    df = df.filter(col("label").isNotNull)

    // =========================
    // CLEAN CATEGORICAL COLUMNS
    // =========================
    val categoricalColsPreferred = Seq(
      "airline_name",
      "status",
      "flight_type",
      "movement_terminal",
      "movement_airport_timeZone",
      "aircraft_model",
      "destination_airport_iata",
      "destination_airport_icao",
      "destination_airport_name"
    )

    val categoricalCols = categoricalColsPreferred.filter(df.columns.contains)

    categoricalCols.foreach { cn =>
      df = df.withColumn(
        cn,
        when(col(cn).isNull || length(trim(col(cn))) === 0, lit("UNKNOWN"))
          .otherwise(trim(col(cn)))
      )
    }

    val upperCols = Seq(
      "destination_airport_iata",
      "destination_airport_icao",
      "movement_terminal"
    ).filter(df.columns.contains)

    upperCols.foreach { cn =>
      df = df.withColumn(cn, upper(col(cn)))
    }

    // =========================
    // TRAIN / TEST SPLIT
    // =========================
    val Array(trainDF, testDF) = df.randomSplit(Array(0.7, 0.3), seed = 42L)

    println("\n========== TRAIN / TEST SPLIT ==========")
    println(s"Train rows: ${trainDF.count()}")
    println(s"Test rows : ${testDF.count()}")

    // =========================
    // BASELINE (MAJORITY CLASS)
    // =========================
    val majorityLabel = trainDF
      .groupBy("label")
      .count()
      .orderBy(desc("count"))
      .first()
      .getAs[Double]("label")

    val baselinePredictions = testDF.withColumn("prediction", lit(majorityLabel))

    val baselineAccuracyEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val baselineAccuracy = baselineAccuracyEvaluator.evaluate(baselinePredictions)

    println("\n========== BASELINE ==========")
    println(s"Majority class label: $majorityLabel")
    println(s"Baseline accuracy   : $baselineAccuracy")

    // =========================
    // FEATURE PIPELINE
    // =========================
    val indexers: Array[StringIndexer] = categoricalCols.map { cn =>
      new StringIndexer()
        .setInputCol(cn)
        .setOutputCol(s"${cn}_idx")
        .setHandleInvalid("keep")
    }.toArray

    val encoder = new OneHotEncoder()
      .setInputCols(categoricalCols.map(cn => s"${cn}_idx").toArray)
      .setOutputCols(categoricalCols.map(cn => s"${cn}_ohe").toArray)
      .setHandleInvalid("keep")

    val numericCols = Seq(
      "hour_of_day",
      "day_of_week",
      "is_weekend",
      "isCargo_int"
    ).filter(trainDF.columns.contains)

    val featureCols = numericCols ++ categoricalCols.map(cn => s"${cn}_ohe")

    val assembler = new VectorAssembler()
      .setInputCols(featureCols.toArray)
      .setOutputCol("features")
      .setHandleInvalid("keep")

    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setProbabilityCol("probability")
      .setRawPredictionCol("rawPrediction")
      .setNumTrees(100)
      .setMaxDepth(10)
      .setSeed(42L)

    val pipeline = new Pipeline().setStages(indexers ++ Array(encoder, assembler, rf))

    // =========================
    // TRAIN MODEL ON TRAIN ONLY
    // =========================
    val pipelineModel: PipelineModel = pipeline.fit(trainDF)

    val trainPredictions = pipelineModel.transform(trainDF)
    val testPredictions  = pipelineModel.transform(testDF)

    // =========================
    // EVALUATION
    // =========================
    val accuracyEval = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val precisionEval = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")

    val recallEval = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")

    val f1Eval = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("f1")

    val aucEval = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")

    val trainAccuracy = accuracyEval.evaluate(trainPredictions)
    val testAccuracy  = accuracyEval.evaluate(testPredictions)

    val testPrecision = precisionEval.evaluate(testPredictions)
    val testRecall    = recallEval.evaluate(testPredictions)
    val testF1        = f1Eval.evaluate(testPredictions)
    val testAUC       = aucEval.evaluate(testPredictions)

    println("\n========== MODEL METRICS ==========")
    println(f"Train Accuracy : $trainAccuracy%.4f")
    println(f"Test Accuracy  : $testAccuracy%.4f")
    println(f"Test Precision : $testPrecision%.4f")
    println(f"Test Recall    : $testRecall%.4f")
    println(f"Test F1-Score  : $testF1%.4f")
    println(f"Test ROC-AUC   : $testAUC%.4f")

    // =========================
    // CONFUSION MATRIX
    // =========================
    println("\n========== CONFUSION MATRIX ==========")
    val confusion = testPredictions
      .groupBy("label", "prediction")
      .count()
      .orderBy("label", "prediction")

    confusion.show(false)

    // =========================
    // FEATURE IMPORTANCE
    // =========================
    val rfModel = pipelineModel.stages.last.asInstanceOf[org.apache.spark.ml.classification.RandomForestClassificationModel]

    println("\n========== FEATURE IMPORTANCES ==========")
    println(s"Input feature groups (assembler inputs): ${featureCols.mkString(", ")}")
    println(s"Random Forest feature importances vector:\n${rfModel.featureImportances}")

    // =========================
    // SAMPLE PREDICTIONS
    // =========================
    println("\n========== SAMPLE PREDICTIONS ==========")
    testPredictions
      .select("label", "prediction", "probability")
      .show(10, truncate = false)

    // =========================
    // SAVE MODEL
    // =========================
    pipelineModel.write.overwrite().save(modelOutPath)
    println(s"Saved trained pipeline model to: $modelOutPath")

    // =========================
    // SAVE METRICS TO TXT
    // =========================
    val writer = new PrintWriter(metricsOut)

    writer.println("RUH Flights - Machine Learning Results")
    writer.println("======================================")
    writer.println(s"Train rows: ${trainDF.count()}")
    writer.println(s"Test rows: ${testDF.count()}")
    writer.println()
    writer.println(s"Majority baseline label: $majorityLabel")
    writer.println(f"Baseline accuracy: $baselineAccuracy%.4f")
    writer.println()
    writer.println(f"Train Accuracy: $trainAccuracy%.4f")
    writer.println(f"Test Accuracy: $testAccuracy%.4f")
    writer.println(f"Test Precision: $testPrecision%.4f")
    writer.println(f"Test Recall: $testRecall%.4f")
    writer.println(f"Test F1-Score: $testF1%.4f")
    writer.println(f"Test ROC-AUC: $testAUC%.4f")
    writer.println()
    writer.println(s"Assembler input feature groups: ${featureCols.mkString(", ")}")
    writer.println(s"Random Forest feature importances vector: ${rfModel.featureImportances}")

    writer.close()

    println(s"Saved metrics to: $metricsOut")

    spark.stop()
  }
}