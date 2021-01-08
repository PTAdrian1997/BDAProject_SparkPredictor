package batch.operations.predict.predictor_class
import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.Calendar

import batch.operations.InputRecord
import batch.operations.predict.predictor_class.MeanPredictorConstFields._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SQLContext, UserDefinedFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructField, StructType, TimestampType}

class MeanPredictor(sqlContext: SQLContext) extends Predictor with java.io.Serializable {

  val generate_next_series: UserDefinedFunction = udf{
    (bestHour: Int, daysDifference: Int, lastTimestamp: Timestamp) =>
      val lastLocalDateTime: LocalDateTime = lastTimestamp.toLocalDateTime
      Seq.range(1, daysDifference + 1).map{
        intVal =>
          Timestamp.valueOf(lastLocalDateTime.getYear + "-" +
            f"${lastLocalDateTime.getMonthValue}%02d" + "-" + f"${lastLocalDateTime.getDayOfMonth + intVal}%02d" +
            " " + f"${bestHour}%02d" + ":00:00")
      }
  }

  /**
   * Predict the optimal time to open the window.
   *
   * @param inputDF The input DataFrame
   * @return a new DataFrame, containing the results of the prediction;
   */
  override def predict(inputDF: DataFrame): DataFrame = {

    val days_difference = new DaysDifference

    // first, get the difference between the maximal date and the minimal date:
//    val timestamps: Array[Timestamp] = inputDF.select(col(TIMESTAMP_COL)).as[Timestamp](Encoders.TIMESTAMP).collect()
//      .sortBy(timestamp => timestamp.getTime)
    val daysDifference: DataFrame = inputDF.select(col(TIMESTAMP_COL)).agg(days_difference(col(TIMESTAMP_COL))
      .as(DAYS_DIFFERENCE_COL))

//    new SQLContext(sparkContext).udf.register("get_mean_noise_hour", new MinMean)
    val get_mean_noise_hour = new MinMean

    // get the last date:
    val lastDate: DataFrame = inputDF.agg(max(col(TIMESTAMP_COL)).as(LAST_TIMESTAMP_COL))

    val noiseMeanPerHour: DataFrame = inputDF
      .drop(col(TEMPERATURE_COL))
      .withColumn(HOUR_COL, hour(col(TIMESTAMP_COL)))
      .drop(col(TIMESTAMP_COL))
      .groupBy(col(HOUR_COL))
      .agg(mean(col(SOUND_VOLUME_COL)).as(MEAN_NOISE_COL))
      .drop(col(SOUND_VOLUME_COL))

    noiseMeanPerHour.show() // for debugging purposes

    // starting from the last date and covering the same time difference as the batch that was processed,
    // use the hour found:
    val bestHour: DataFrame = noiseMeanPerHour.agg(get_mean_noise_hour(col(HOUR_COL), col(MEAN_NOISE_COL))
      .as(BEST_HOUR_COL))

    val newRDD: RDD[Row] = bestHour.join(daysDifference).join(lastDate).flatMap {
      row =>
        val bestHourEl: Int = row.getInt(0)
        val daysDifferenceEl: Int = row.getInt(1)
        val lastDateEl: Timestamp = row.getTimestamp(2)
        val lastLocalDateTime: LocalDateTime = lastDateEl.toLocalDateTime
        Seq.range(1, daysDifferenceEl + 1).map { intNumber =>
          val newLocalDT: LocalDateTime = lastLocalDateTime.plusDays(intNumber).withHour(bestHourEl)
          Row(Timestamp.valueOf(newLocalDT.getYear + "-" +
            f"${newLocalDT.getMonthValue}%02d" + "-" + f"${newLocalDT.getDayOfMonth}%02d" +
            " " + f"${bestHourEl}%02d" + ":00:00"))
        }
    }
    sqlContext.createDataFrame(newRDD, StructType(StructField(TIMESTAMP_COL, TimestampType,
      nullable = false) :: Nil))

  }

  class MinMean extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = StructType(
      StructField(HOUR_COL, IntegerType, nullable = false) ::
        StructField(MEAN_NOISE_COL, DoubleType, nullable = false)
      :: Nil)

    override def bufferSchema: StructType = StructType(
      StructField(BEST_HOUR_COL, IntegerType, nullable = false) ::
        StructField(BEST_HOUR_VALUE, DoubleType, nullable = false) ::
        Nil
    )

    override def dataType: DataType = IntegerType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = -1
      buffer(1) = 100000.0
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val currentHour: Int = input.getAs[Int](0)
      val currentNoise: Double = input.getAs[Double](1)
      if(buffer(0) == -1 || buffer.getAs[Double](1) > currentNoise){
        buffer(0) = currentHour
        buffer(1) = currentNoise
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val (buffer1Hour: Int, buffer1Noise: Double) = (buffer1.getAs[Int](0), buffer1.getAs[Double](1))
      val (buffer2Hour: Int, buffer2Noise: Double) = (buffer2.getAs[Int](0), buffer2.getAs[Double](1))
      val (newH, newNoise) = if (buffer1Noise > buffer2Noise) {
        (buffer2Hour, buffer2Noise)
      } else {
        (buffer1Hour, buffer1Noise)
      }
      buffer1(0) = newH
      buffer1(1) = newNoise
    }

    override def evaluate(buffer: Row): Any = {
      val (bufferHour: Int, bufferNoise: Double) = (buffer.getAs[Int](0), buffer.getAs[Double](1))
      bufferHour
    }
  }

  class DaysDifference extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = StructType(StructField(TIMESTAMP_COL, TimestampType, nullable = false) ::
      Nil)

    override def bufferSchema: StructType = StructType(
      StructField(MIN_TIMESTAMP, TimestampType, nullable = false) ::
        StructField(MAX_TIMESTAMP, TimestampType, nullable = false) :: Nil
    )

    override def dataType: DataType = IntegerType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = Timestamp.valueOf("1970-01-01 00:00:00")
      buffer(1) = Timestamp.valueOf("1970-01-01 00:00:00")
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val currentTimestamp = input.getTimestamp(0)
      val (bufferMinTimestamp, bufferMaxTimestamp) = (buffer.getTimestamp(0), buffer.getTimestamp(1))
      buffer(0) = if(bufferMinTimestamp.after(currentTimestamp)) currentTimestamp else bufferMinTimestamp
      buffer(1) = if(bufferMaxTimestamp.before(currentTimestamp)) currentTimestamp else bufferMaxTimestamp
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val (buffer1MinTimestamp, buffer1MaxTimestamp) = (buffer1.getTimestamp(0), buffer1.getTimestamp(1))
      val (buffer2MinTimestamp, buffer2MaxTimestamp) = (buffer2.getTimestamp(0), buffer2.getTimestamp(1))
      buffer1(0) = if(buffer1MinTimestamp.after(buffer2MinTimestamp)) buffer2MinTimestamp else buffer1MinTimestamp
      buffer1(1) = if(buffer1MaxTimestamp.before(buffer2MaxTimestamp)) buffer2MaxTimestamp else buffer1MaxTimestamp
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getTimestamp(1).toLocalDateTime.getDayOfYear - buffer.getTimestamp(0).toLocalDateTime.getDayOfYear
    }
  }

}
