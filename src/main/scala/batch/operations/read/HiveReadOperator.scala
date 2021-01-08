package batch.operations.read

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/**
 *
 * @param sparkContext
 */
class HiveReadOperator(sparkContext: SparkContext) extends ReadOperator {

  /**
   *
   * @param tableName
   * @param databaseName
   * @return
   */
  def readFromHiveTable(tableName: String, databaseName: String): DataFrame = {
    val hiveContext: HiveContext = new HiveContext(sparkContext)
//    hiveContext.table(s"`$databaseName.$tableName`")
    new SQLContext(sparkContext).sql("select * from `team5_2020_impala_db.simulated_batch`")
  }

  /**
   * Read the raw, input dataframe.
   *
   * @param configurationValues the variables needed to handle the DataFrame reading operation.
   * @return the input DataFrame.
   */
  override def readInputDF(configurationValues: Map[String, String]): DataFrame = {
    val tableName = configurationValues.getOrElse(TABLE_NAME_STRING, "")
    val databaseName = configurationValues.getOrElse(DATABASE_NAME_STRING, "")
    readFromHiveTable(tableName, databaseName)
  }

}
