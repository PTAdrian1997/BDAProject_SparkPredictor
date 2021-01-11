package application

import java.sql.Timestamp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType, TimestampType}

object ExampleRun {

  def runExample: Unit = {
    val conf: SparkConf = new SparkConf().setAppName("OptimalWindowPredictor").setMaster("local")
    val sparkContext: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sparkContext)

    val tableName: String = "merg"
    val dbName: String = "dbName"

    val schema: StructType = new StructType()
      .add(StructField("timestamp", TimestampType, nullable = false))
      .add(StructField("temperature", DoubleType, nullable = false))
      .add(StructField("sound_volume", DoubleType, nullable = false))

    val rowsRDD: RDD[Row] = sparkContext.parallelize(Seq(
      Row(Timestamp.valueOf("2020-11-20 12:32:00"), -10.24, 543.21),
      Row(Timestamp.valueOf("2020-11-21 12:32:00"), 1.45, 143.26),
      Row(Timestamp.valueOf("2020-11-22 12:32:00"), 20.0, 100.00)
    ))

    val rowsDF: DataFrame = sqlContext.createDataFrame(rowsRDD, schema)
    // write the data:
//    rowsDF.write.parquet("/home/team5_2020/pahp1871/deploy_job/run_example_output")
    rowsDF.write.parquet("output")
    rowsDF.show()
  }

}
