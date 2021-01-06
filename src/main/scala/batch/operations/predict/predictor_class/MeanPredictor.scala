package batch.operations.predict.predictor_class
import java.sql.Timestamp

import batch.operations.InputRecord
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructField, StructType}

class MeanPredictor(sparkContext: SparkContext) extends Predictor with java.io.Serializable {

  /**
   * Predict the optimal time to open the window.
   *
   * @param inputDF The input DataFrame
   * @return a new DataFrame, containing the results of the prediction;
   */
  override def predict(inputDF: DataFrame): DataFrame = {

    // first, get the difference between the maximal date and the minimal date:
    val timestamps: Array[Timestamp] = inputDF.select(col(TIMESTAMP_COL)).as[Timestamp](Encoders.TIMESTAMP).collect()
      .sortBy(timestamp => timestamp.getTime)
    val daysDifference: Long = timestamps.last.toLocalDateTime.getDayOfYear - timestamps.head.toLocalDateTime
      .getDayOfYear

//    new SQLContext(sparkContext).udf.register("get_mean_noise_hour", new MinMean)
    val get_mean_noise_hour = new MinMean

    val noiseMeanPerHour: DataFrame = inputDF
      .drop(col(TEMPERATURE_COL))
      .withColumn(HOUR_COL, hour(col(TIMESTAMP_COL)))
//      .withColumn(DATE_COL, to_date(col(TIMESTAMP_COL)))
      .drop(col(TIMESTAMP_COL))
      .groupBy(col(HOUR_COL))
      .agg(mean(col(SOUND_VOLUME_COL)).as(MEAN_NOISE_COL))
      .drop(col(SOUND_VOLUME_COL))

    noiseMeanPerHour.show() // for debugging purposes
    noiseMeanPerHour.agg(get_mean_noise_hour(col(HOUR_COL), col(MEAN_NOISE_COL)))

  }

  class MinMean extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = StructType(
      StructField(HOUR_COL, IntegerType, nullable = false) ::
        StructField(MEAN_NOISE_COL, DoubleType, nullable = false)
      :: Nil)

    override def bufferSchema: StructType = StructType(
      StructField("best_hour", IntegerType, nullable = false) ::
        StructField("current_best_value", DoubleType, nullable = false) ::
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
      val (newH, newNoise) = buffer1Noise > buffer2Noise match {
        case true => (buffer2Hour, buffer2Noise)
        case _ => (buffer1Hour, buffer1Noise)
      }
      buffer1(0) = newH
      buffer1(1) = newNoise
    }

    override def evaluate(buffer: Row): Any = {
      val (bufferHour: Int, bufferNoise: Double) = (buffer.getAs[Int](0), buffer.getAs[Double](1))
      bufferHour
    }
  }

}
