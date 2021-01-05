package batch.operations.predict.predictor_class


import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar

import application.MarkovianPredictorConfig
import batch.operations.InputRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

import scala.collection.mutable

class MarkovianPredictor(sparkContext: SparkContext, predictorConfig: MarkovianPredictorConfig)
  extends Predictor
  with LearnOperator {

  /**
   * Learn new patterns from the current batch
   * @param inputDF the input dataframe from the current batch
   * @return the new pattern
   */
  override def learn(inputDF: DataFrame): DataFrame = {
    val newRDD: RDD[Row] = inputDF
      .withColumn(DATE_COL, to_date(col(TIMESTAMP_COL)))
      .withColumn(ZIPPED_TIME_VALUE_COL,
        struct(
          get_day_moment_in_seconds(col(TIMESTAMP_COL)),
          col(TEMPERATURE_COL),
          col(SOUND_VOLUME_COL)
        )
      )
      .drop(col(TIMESTAMP_COL))
      .groupBy(col(DATE_COL)).agg(collect_list(col(ZIPPED_TIME_VALUE_COL)))
      .map{case Row(date: Date,  listOfTuples: Seq[(Long, Double, Double)]) =>
        // choose the best time interval for each date:
        val groupedData: Map[Long, Double] = listOfTuples
        .groupBy(tupleElement => tupleElement._1 / predictorConfig.predictorGranularity)
        .mapValues{seq =>
          val squaredSum: Double = seq.map{case (timeValue: Long, tempVal: Double, soundVal: Double) =>
            (tempVal - soundVal) * (tempVal - soundVal)
          }.sum
          squaredSum
        }
        val bestTimeInterval: Long = groupedData.toList.maxBy(_._2)._1
        Row(date: Date, bestTimeInterval: Long)
      }
//      .groupBy[](row: Row => row)
    val structType: StructType = StructType(Seq(
      StructField(DATE_COL, DateType, nullable = false),
      StructField(OPTIMAL_TIME_REGION_COL, LongType, nullable = false)
    ))
    new SQLContext(sparkContext).createDataFrame(newRDD, structType)
  }

  /**
   * UserDefinedFunction to get the total number of seconds for the given
   * moment in a day (the day is represented by a timestamp)
   */
  private val get_day_moment_in_seconds = udf {
    timestamp: Timestamp =>
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTimeInMillis(timestamp.getTime)
      calendar.get(Calendar.HOUR_OF_DAY) * 3600 + calendar.get(Calendar.MINUTE) * 60 + calendar.get(Calendar.SECOND)
  }

  private case class InputRecord(col1: Long, temperaturevalue: Double, soundvalue: Double)

  /**
   *
   */
  private val get_best_moments_for_a_day = udf {
    listOfTuples: mutable.WrappedArray[Row] =>
      listOfTuples.map{r => (r.getAs[Int](0), r.getAs[Double](1), r.getAs[Double](2))}
        .groupBy(tupleElement => tupleElement._1 / predictorConfig.predictorGranularity)
        .mapValues { seq =>
          val squaredSum: Double = seq.map { case (timeValue: Int, tempVal: Double, soundVal: Double) =>
            (tempVal - soundVal) * (tempVal - soundVal)
          }.sum
          squaredSum
        }
      .toList.maxBy(_._2)._1
  }

  /**
   *
   * @param predictorGranularity
   * @return
   */
  private def getBestMomentForADay(predictorGranularity: Int) = udf {
    listOfTuples: mutable.WrappedArray[Row] =>
      listOfTuples.map{r => (r.getAs[Int](0), r.getAs[Double](1), r.getAs[Double](2))}
        .groupBy(tupleElement => tupleElement._1 / predictorGranularity)
        .mapValues { seq =>
          val squaredSum: Double = seq.map { case (timeValue: Int, tempVal: Double, soundVal: Double) =>
            (tempVal - soundVal) * (tempVal - soundVal)
          }.sum
          squaredSum
        }
        .toList.maxBy(_._2)._1
  }

  /**
   * Predict the optimal time to open the window.
   *
   * @param inputDF The input DataFrame
   * @return a new DataFrame, containing the results of the prediction;
   */
  override def predict(inputDF: DataFrame): DataFrame = {
    inputDF
  }

}
