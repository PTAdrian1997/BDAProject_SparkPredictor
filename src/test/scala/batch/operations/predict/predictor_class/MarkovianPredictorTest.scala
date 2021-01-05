package batch.operations.predict.predictor_class

import java.sql.Timestamp
import java.util.Random

import batch.operations.InputRecord
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest._
import org.scalatest.funspec.AnyFunSpec

import scala.collection.mutable.Stack

class MarkovianPredictorTest extends AnyFunSpec with DatasetSuiteBase {

//  val configuration: SparkConf = new SparkConf().setAppName("OptimalWindowPredictor").setMaster("local")
//  val sparkContext: SparkContext = new SparkContext(conf)
//  override lazy val sqlContext: SQLContext = new SQLContext(sparkContext)

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

  describe("MarkovianPredictor.get_best_moments_for_a_day"){
    it("must extract from a column of type List[(milliseconds_in_day, temperature, sound)] the best time period"){
      val startTimestamp: Double = 1606155.072000
      val maxRandInt: Int = 200
      val tempStrength: Double = 3.5
      val soundStrength: Double = 3.5
      val numberOfElementsInSeq: Int = 10000
      val inputDF: DataFrame = new RandomInputGenerator(startTimestamp, maxRandInt, tempStrength, soundStrength,
        numberOfElementsInSeq).getDataFrame
      println(inputDF.count())
    }
  }

}
