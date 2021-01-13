package batch.operations.predict.predictor_class
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.regression.LinearRegression

class TimeSeriesPredictor {

  def learn(df: DataFrame): Array[Double] = {

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val lrModel = lr.fit(df)
    Array.empty[Double]
  }
}
