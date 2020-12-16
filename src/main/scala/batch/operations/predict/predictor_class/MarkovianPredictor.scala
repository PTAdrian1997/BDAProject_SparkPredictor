package batch.operations.predict.predictor_class

import batch.operations.InputRecord
import org.apache.spark.sql.{DataFrame, Dataset}

class MarkovianPredictor extends Predictor {
  /**
   * Predict the optimal time to open the window.
   *
   * @param inputDS The input DataFrame
   * @return a new DataFrame, containing the results of the prediction;
   */
  override def predict(inputDS: DataFrame): DataFrame = ???
}
