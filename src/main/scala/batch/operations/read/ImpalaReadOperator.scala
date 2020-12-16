package batch.operations.read

import java.util.Properties

import application.ReadOperatorConfig
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

class ImpalaReadOperator(sqlContext: SQLContext) extends ReadOperator {

  import sqlContext.implicits._

  /**
   * Read the data from the input table
   * @param inputTable the path to the table (it must also contain the database)
   * @return a DataFrame containing all the information from the source
   */
  def readImpalaDF(connectionURL: String, inputTable: String, inputDatabase: String): DataFrame = {
    Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance
    sqlContext.read.jdbc(connectionURL, s"$inputTable", new Properties())
  }

  /**
   * Read the raw, input dataframe.
   *
   * @param configurationValues the variables needed to handle the DataFrame reading operation.
   * @return the input DataFrame.
   */
  override def readInputDF(configurationValues: Map[String, String]): DataFrame = {
    // extract the variables:
    val url: String = configurationValues.getOrElse("url", "")
    val inputTable: String = configurationValues.getOrElse("inputTable", "")
    val inputDatabase: String = configurationValues.getOrElse("inputDatabase", "")
    readImpalaDF(url, inputTable, inputDatabase)
  }
}
