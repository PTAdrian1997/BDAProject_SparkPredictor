package application

import java.sql.Timestamp

import batch.operations.read.{HiveReadOperator, ImpalaReadOperator}
import batch.operations.write.WriteOperator
import org.apache.spark.sql.SQLContext

import scala.collection.immutable.HashMap

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

  val tableName: String = "merge"
  val dbName: String = "dbName"

  // read the input data:
  val readVariables: Map[String, String] = HashMap(
    ("url", readOperatorConfigElements.url),
    ("inputTable", readOperatorConfigElements.inputTable),
    ("inputDatabase", readOperatorConfigElements.inputDatabase))
  val inputDF: DataFrame = new HiveReadOperator(sparkContext).readInputDF(readVariables)
  inputDF.count()

  // write the output data to the table:
//  WriteOperator.writeOutputDF(inputDF, writeOperatorConfigElements.outputPath)

}
