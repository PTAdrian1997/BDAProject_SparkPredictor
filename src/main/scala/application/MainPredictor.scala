package application

import java.sql.Timestamp

import batch.operations.read.{HiveReadOperator, ImpalaReadOperator}
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
//
//  val tableName: String = "merge"
//  val dbName: String = "dbName"

  // read the input data:
  val readVariables: Map[String, String] = HashMap(
    ("url", readOperatorConfigElements.url),
    ("inputTable", readOperatorConfigElements.inputTable),
    ("inputDatabase", readOperatorConfigElements.inputDatabase))
  val inputDF: DataFrame = new HiveReadOperator(sparkContext).readInputDF(readVariables)
//  inputDF.count()

  // write the output data to the table:
//  WriteOperator.writeOutputDF(inputDF, writeOperatorConfigElements.outputPath)

//  case class InputRecord(timestamp: Timestamp, temperatureValue: Double, soundValue: Double)
//  val random: Random = new Random()
//  val startTimestamp: Double = 1606155.072000
//  val maxRandInt: Int = 200
//  val tempStrength: Double = 3.5
//  val soundStrength: Double = 3.5
//  val numberOfElementsInSeq: Int = 10000
//  val inputSeq: Seq[InputRecord] = Seq.fill[InputRecord](numberOfElementsInSeq){
//    val newTimestamp: Double = startTimestamp + random.nextInt(maxRandInt)
//    val newTemp: Double = random.nextDouble * tempStrength
//    val newSound: Double = random.nextDouble * soundStrength
//    InputRecord(new Timestamp((newTimestamp * 1000000).toLong), newTemp, newSound)
//  }
//
//  val dummyRDD: RDD[InputRecord] = sparkContext.parallelize(inputSeq)
//  val dummyDF: DataFrame = sqlContext.createDataFrame(dummyRDD)
//  dummyDF.write.saveAsTable("team5_2020_impala_db.stupid_data")

  ExampleRun.runExample

}
