package batch.operations.predict.predictor_class

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.Random

import batch.operations.InputRecord
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBase}
import org.apache.spark.sql.functions.{col, hour, max, mean}
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.scalatest
import org.scalatest.{FunSpec, Matchers}

class MeanPredictorTest  extends FunSpec with DataFrameSuiteBase with Matchers {

  import sqlContext.implicits._

  /**
   * class that will generate a random input DataFrame for testing purposes
   * @param startTimestamp
   * @param maxRandInt
   * @param tempStrength
   * @param soundStrength
   * @param numberOfElements
   */
  class RandomInputGenerator(startTimestamp: Double, maxRandInt: Int, tempStrength: Double,
                             soundStrength: Double, numberOfElements: Int){
    val random: Random = new Random()

    def computeInputSeq: Seq[InputRecord] = Seq.fill[InputRecord](numberOfElements){
      val newTimestamp: Double = startTimestamp + random.nextInt(maxRandInt)
      val newTemp: Double = random.nextDouble * tempStrength
      val newSound: Double = random.nextDouble * soundStrength
      InputRecord(new Timestamp((newTimestamp * 1000000).toLong), newTemp, newSound)
    }

    def getDataFrame: DataFrame = sqlContext.createDataFrame(sc.parallelize(computeInputSeq))

  }

  describe("MeamPredictor.predict"){
    it("must extract the correct mean from the previous days"){
      val startTimestamp: Double = 1606155.072000
      val maxRandInt: Int = 200
      val tempStrength: Double = 3.5
      val soundStrength: Double = 3.5
      val numberOfElementsInSeq: Int = 10
      val inputDF: DataFrame = new RandomInputGenerator(startTimestamp, maxRandInt, tempStrength, soundStrength,
        numberOfElementsInSeq).getDataFrame

      val meanPredictor: MeanPredictor = new MeanPredictor(sqlContext)
      val newDF: DataFrame = meanPredictor.predict(inputDF)
    }
    it("must return a DataFrame that has the same number of elements as the input" +
      " DataFrame and with hour=the hour with the smallest noise"){
      val inputSeq: Seq[InputRecord] = Seq(
        InputRecord(Timestamp.valueOf("2021-01-07 11:00:01"), 43.25, 21.57),
        InputRecord(Timestamp.valueOf("2021-01-07 11:00:08"), 35.65, 25.43),
        InputRecord(Timestamp.valueOf("2021-01-07 12:01:43"), 43.24, 23.54),
        InputRecord(Timestamp.valueOf("2021-01-08 11:03:03"), 10.94, 43.53),
        InputRecord(Timestamp.valueOf("2021-01-09 12:23:00"), 11.43, 11.06),
        InputRecord(Timestamp.valueOf("2021-01-09 12:43:00"), 09.64, 19.23),
        InputRecord(Timestamp.valueOf("2021-01-10 12:10:00"), 11.94, 20.21)
      )
      val inputDF: DataFrame = sqlContext.createDataFrame(inputSeq)
      val expectedSeq: Seq[Row] = Seq(
        Row(Timestamp.valueOf("2021-01-11 12:00:00.0")),
        Row(Timestamp.valueOf("2021-01-12 12:00:00.0")),
        Row(Timestamp.valueOf("2021-01-13 12:00:00.0"))
      )
      val expectedStructType: StructType = StructType(StructField("timestamp", TimestampType, nullable = false) :: Nil)
      val expectedDF: DataFrame = sqlContext.createDataFrame(sqlContext.sparkContext.parallelize(expectedSeq),
        expectedStructType)
      val meanPredictor: MeanPredictor = new MeanPredictor(sqlContext)
      val outputDF: DataFrame = meanPredictor.predict(inputDF)
      assertDataFrameEquals(expectedDF, outputDF)
    }

    it("must return an empty DataFrame if the input DataFrame is empty"){
      val meanPredictor: MeanPredictor = new MeanPredictor(sqlContext)
      val outputDF: DataFrame = meanPredictor.predict(sqlContext.emptyDataFrame)
      assertDataFrameEquals(sqlContext.emptyDataFrame, outputDF)
    }

  }

}
