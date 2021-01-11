package application

import java.sql.Timestamp

import batch.operations.read.{DATABASE_NAME_STRING, HiveReadOperator, ImpalaReadOperator, TABLE_NAME_STRING, URL_STRING}
import batch.operations.write.WriteOperator
import org.apache.spark.sql.SQLContext

import scala.collection.immutable.HashMap
import scala.util.Random

//import batch.operations.read.ReadOperator
//import batch.operations.write.WriteOperator
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType};

object MainPredictor extends App {

  val conf: SparkConf = new SparkConf().setAppName("OptimalWindowPredictor")
  val sparkContext: SparkContext = new SparkContext(conf)
  val sqlContext: SQLContext = new SQLContext(sparkContext)

  // read the input data:
  val readVariables: Map[String, String] = HashMap(
    (URL_STRING, readOperatorConfigElements.url),
    (TABLE_NAME_STRING, readOperatorConfigElements.inputTable),
    (DATABASE_NAME_STRING, readOperatorConfigElements.inputDatabase))
  val inputDF: DataFrame = new HiveReadOperator(sparkContext).readInputDF(readVariables)
  println(inputDF.count())

}
