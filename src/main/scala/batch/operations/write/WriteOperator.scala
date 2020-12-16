package batch.operations.write

import org.apache.spark.sql.{DataFrame, SQLContext}

object WriteOperator {

  def writeOutputDF(outputDataFrame: DataFrame, outputPath: String): Unit = {
    outputDataFrame.write.saveAsTable(outputPath)
  }

}
