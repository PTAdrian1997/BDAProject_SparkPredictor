package batch.operations.predict.predictor_class

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, UserDefinedFunction}
import org.scalatest.{FunSpec, Matchers}
import org.apache.spark.mllib._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.LocalDateTime

class TimeSeriesPredictorTest extends FunSpec with DatasetSuiteBase with Matchers {

  val convert_features_for_ml: UserDefinedFunction = udf{
    inputTimestamp: Timestamp =>
      val inputLocalDateTime: LocalDateTime = inputTimestamp.toLocalDateTime
      val inputYear = inputLocalDateTime.getYear
      val inputDay = inputLocalDateTime.getDayOfYear
      val inputMillisecond = inputLocalDateTime.getHour * 60 * 60 * 1000 + inputLocalDateTime.getMinute * 60 * 1000
      + inputLocalDateTime.getSecond * 1000
      Vectors.dense(inputYear, inputDay, inputMillisecond)
  }

  describe("TimeSeriesPredictor.learn"){
    it("should be able to forecast time series"){
      val inputSeq: Seq[Row] = Seq(
        Row(Timestamp.valueOf("2020-10-11 10:00:11"), 23.4),
        Row(Timestamp.valueOf("2020-10-11 10:00:13"), 25.4),
        Row(Timestamp.valueOf("2020-10-12 10:00:14"), 26.78))
      val inputSchema: StructType = StructType(
        StructField("timestamp", TimestampType, nullable = false) ::
          StructField("tempVal", DoubleType, nullable = false) :: Nil)
        val inputRDD = sqlContext.sparkContext.parallelize(inputSeq)
        val inputDF: DataFrame = sqlContext.createDataFrame(inputRDD, inputSchema)

      // convert the input DataFrame into a form that can be processed by apache spark ml:

      val inputMLDF: DataFrame = inputDF
        .withColumn("features", convert_features_for_ml(col("timestamp")))
      inputMLDF.printSchema()
      inputMLDF.show(false)

//      val lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
//      val lrModel = lr.fit(inputDF)
//      println(lrModel.summary)
    }
  }

}
