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
   * Learn new patterns from the current batch.
   * In this specific case (Markovian predictor), it should return, for
   * each unique pair (interval_i_id, interval_j_id), the probability that,
   * if on day x, the best interval to open the window is i, then the best
   * interval to open the window on day x+1 will be j
   * @param inputDF the input dataframe from the current batch
   * @return the new pattern
   */
  override def learn(inputDF: DataFrame): DataFrame = {
    val bestMomentsDF: DataFrame = inputDF
      .withColumn(DATE_COL, to_date(col(TIMESTAMP_COL)))
      .withColumn(ZIPPED_TIME_VALUE_COL,
        struct(
          get_day_moment_in_seconds(col(TIMESTAMP_COL)),
          col(TEMPERATURE_COL),
          col(SOUND_VOLUME_COL)
        )
      )
      .drop(col(TIMESTAMP_COL))
      .groupBy(col(DATE_COL)).agg(collect_list(col(ZIPPED_TIME_VALUE_COL)).alias(ZIPPED_TIME_VALUE_LIST_COL))
      .withColumn(OPTIMAL_TIME_REGION_COL, get_best_moments_for_a_day(col(ZIPPED_TIME_VALUE_LIST_COL)))
      .drop(col(ZIPPED_TIME_VALUE_LIST_COL))
    bestMomentsDF
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
   * Get the best time interval for a day
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
   * Predict the optimal time to open the window.
   *
   * @param inputDF The input DataFrame
   * @return a new DataFrame, containing the results of the prediction;
   */
  override def predict(inputDF: DataFrame): DataFrame = {
    inputDF
  }

}
