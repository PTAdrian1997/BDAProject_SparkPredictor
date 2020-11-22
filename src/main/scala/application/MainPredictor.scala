package application

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType};

object MainPredictor extends App {

  val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("predictor")
    .config("spark.master", "local")
    .getOrCreate()

  val tableName: String = "merg"
  val dbName: String = "dbName"

  val schema: StructType = new StructType()
    .add(StructField("timestamp", TimestampType, nullable = false))
    .add(StructField("temperature", DoubleType, nullable = false))
    .add(StructField("sound_volume", DoubleType, nullable = false))

  val rowsRDD: RDD[Row] = sparkSession.sparkContext.parallelize(Seq(
    Row(Timestamp.valueOf("2020-11-20 12:32:00"), -10.24, 543.21),
    Row(Timestamp.valueOf("2020-11-21 12:32:00"), 1.45, 143.26),
    Row(Timestamp.valueOf("2020-11-22 12:32:00"), 20.0, 100.00)
  ))

  val rawData: DataFrame = sparkSession.createDataFrame(rowsRDD, schema)
  rawData.show()

}
