package batch.operations.predict.predictor_class

import org.apache.spark.sql.DataFrame

/**
 * The Predictor trait: all the available predictors must implement this trait.
 */
trait Predictor {

  /**
   * Predict the optimal time to open the window.
   * @param inputDF The input DataFrame
   * @return a new DataFrame, containing the results of the prediction;
   */
  def predict(inputDF: DataFrame): DataFrame

}
