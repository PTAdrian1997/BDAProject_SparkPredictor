package batch.operations.predict.predictor_class

import org.apache.spark.sql.DataFrame

trait LearnOperator {

  /**
   * Learn new patterns from the current batch
   * @param df the input dataframe from the current batch
   * @return the new pattern
   */
  def learn(df: DataFrame): DataFrame

}
