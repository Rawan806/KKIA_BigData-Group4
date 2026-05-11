# RUH Flights Big Data Project - Peak Traffic Pattern Group
========================================================

1. Project Overview
-------------------
This project analyzes operational flight traffic at King Khalid International Airport (RUH) using Apache Spark and Scala. The main objective is to identify and predict peak operational traffic windows based on scheduled flight data, temporal patterns, airline information, destination routes, and terminal activity.

The project pipeline includes:
1. Data preprocessing
2. Data reduction
3. Data transformation
4. RDD operations
5. Spark SQL operations
6. Machine learning using Spark MLlib


2. Environment Setup
--------------------
Required software:

- Java JDK 11
- Apache Spark 3.5.1
- Scala 2.12.x
- sbt 1.x
- IntelliJ IDEA or any Scala-compatible IDE

Recommended Java version:
- JDK 11

Recommended Spark version:
- Apache Spark 3.5.1

Recommended Scala version:
- Scala 2.12.x

Notes for Windows users:
- If Spark raises a Hadoop or winutils-related warning/error, configure HADOOP_HOME and add winutils.exe if model/file saving is required.
- The main analysis results can still be generated even if Spark fails only at the final model-saving step.


3. Project Folder Structure
---------------------------
The submitted project folder should follow this structure:

GroupName_BigDataProject/
│
├── README.txt
├── FinalReport.pdf
├── Presentation_slides.pdf
│
├── code/
│   ├── DataPreprocessingRUHFlights.scala
│   ├── DataReductionRUHFlights.scala
│   ├── DataTransformationRUHFlights.scala
│   ├── RDDOperationsRUHFlights.scala
│   ├── SQLOperationsRUHFlights.scala
│   └── MachineLearningRUHFlights.scala
│
├── data/
│   ├── raw/
│   │   └── flights_RUH.parquet
│   └── processed/
│
└── results/
    ├── rdd_output.txt
    ├── sql_results.csv
    └── ml_metrics.txt


4. Dataset
----------
Input dataset:
data/raw/flights_RUH.parquet

The dataset contains scheduled flight operation records for King Khalid International Airport (RUH). It includes flight information, airline details, aircraft details, origin/destination airport fields, scheduled timestamps, terminal information, status, and flight type.

Dataset size:
- 153,308 records
- Parquet format


5. How to Run the Scripts
-------------------------

Run the scripts in the following order because later phases depend on the outputs and logic from earlier phases.


Step 1: Data Preprocessing
--------------------------
Main class:
DataPreprocessingRUHFlights

Purpose:
- Load raw RUH flights dataset
- Clean null-like values
- Standardize text and code columns
- Parse timestamps
- Generate temporal features
- Create operational traffic windows
- Generate peak_traffic_label

Run command:

sbt "runMain DataPreprocessingRUHFlights"


Step 2: Data Reduction
----------------------
Main class:
DataReductionRUHFlights

Purpose:
- Reduce the dataset from 33 columns to 14 selected columns
- Remove redundant identifiers and low-value metadata
- Preserve all 153,308 rows

Run command:

sbt "runMain DataReductionRUHFlights"


Step 3: Data Transformation
---------------------------
Main class:
DataTransformationRUHFlights

Purpose:
- Rename columns with dots to Spark-friendly names
- Encode categorical variables
- Assemble ML features into a vector
- Create final Spark ML schema with label and features

Run command:

sbt "runMain DataTransformationRUHFlights"


Step 4: RDD Operations
----------------------
Main class:
RDDOperationsRUHFlights

Purpose:
- Perform Spark RDD transformations and actions
- Analyze hourly traffic, weekday/weekend activity, peak/non-peak counts, top airlines, top destinations, routes, and traffic patterns

Run command:

sbt "runMain RDDOperationsRUHFlights"

Output:
results/rdd_output.txt


Step 5: SQL Operations
----------------------
Main class:
SQLOperationsRUHFlights

Purpose:
- Register the preprocessed dataset as a temporary SQL view
- Run advanced SQL queries including:
  - rolling 3-hour average
  - LAG-based hour-over-hour surge detection
  - airline peak concentration ratio
  - top peak destination per airline
  - GROUPING SETS
  - percentile-based peak classification
  - weekday vs weekend average traffic
  - DENSE_RANK for peak destinations

Run command:

sbt "runMain SQLOperationsRUHFlights"

Output:
results/sql_results.csv


Step 6: Machine Learning
------------------------
Main class:
MachineLearningRUHFlights

Purpose:
- Train a Random Forest classifier
- Predict peak operational traffic windows
- Compare model performance to majority-class baseline
- Evaluate using accuracy, precision, recall, F1-score, ROC-AUC, and confusion matrix

Run command:

sbt "runMain MachineLearningRUHFlights"

Output:
results/ml_metrics.txt


6. Machine Learning Summary
---------------------------
Target variable:
peak_traffic_label

Meaning:
- 1 = peak operational traffic window
- 0 = non-peak operational traffic window

Model:
Random Forest Classifier

Train/test split:
- 70% training
- 30% testing

Main evaluation results:
- Baseline Accuracy: 0.5921
- Train Accuracy: 0.9207
- Test Accuracy: 0.9235
- Precision: 0.9321
- Recall: 0.9235
- F1-Score: 0.9241
- ROC-AUC: 0.9578


7. Special Dependencies and Notes
---------------------------------
Required Spark libraries:
- spark-sql
- spark-core
- spark-mllib

The project uses:
- Spark DataFrame API
- Spark SQL
- Spark RDD API
- Spark MLlib / ML Pipeline

If running on Windows:
- Use Java 11.
- Make sure Spark and Scala versions are compatible.
- Hadoop/winutils setup may be needed if saving models or Spark output folders causes a HADOOP_HOME error.

If the trained model saving step fails because HADOOP_HOME is unset, this does not affect the generated model metrics as long as training and evaluation were completed successfully.


8. Expected Result Files
------------------------
After running the project scripts, the results folder should contain:

results/rdd_output.txt
- RDD operation outputs and short interpretations

results/sql_results.csv
- Spark SQL query results

results/ml_metrics.txt
- Machine learning evaluation metrics and interpretation


9. Notes
--------
All code was implemented using Apache Spark with Scala. The project demonstrates a complete big data workflow from raw data preprocessing to distributed analysis and machine learning-based prediction.
