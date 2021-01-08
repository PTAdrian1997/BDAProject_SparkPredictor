package batch.operations.read

import org.apache.spark.sql.DataFrame

trait ReadOperator {

  /**
   * Read the raw, input dataframe.
   * @param configurationValues the variables needed to handle the DataFrame reading operation.
   * @return the input DataFrame.
   */
  def readInputDF(configurationValues: Map[String, String]): DataFrame

}
