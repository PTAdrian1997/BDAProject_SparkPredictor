package batch.operations.read

import org.apache.spark.sql.DataFrame

trait ReadOperator {

  val TABLE_NAME_STRING: String = "inputTable"
  val DATABASE_NAME_STRING: String = "inputDatabase"
  val URL_STRING: String = "url"

  /**
   * Read the raw, input dataframe.
   * @param configurationValues the variables needed to handle the DataFrame reading operation.
   * @return the input DataFrame.
   */
  def readInputDF(configurationValues: Map[String, String]): DataFrame

}
