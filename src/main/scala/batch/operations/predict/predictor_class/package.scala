package batch.operations.predict

package object predictor_class {
  //val VARIABLE_NAME_COL: String = "variable_name"
  val TIMESTAMP_COL: String = "timestamp"
  val TEMPERATURE_COL: String = "temperature_value"
  val SOUND_VOLUME_COL: String = "sound_volume"
  //val VALUE_COL: String = "value"

  val DATE_COL: String = "date"
  val CURRENT_SECONDS_COL: String = "current_seconds"
  val PER_DAY_TIME_SERIES_COL: String = "mapped_col"
  val ZIPPED_TIME_VALUE_COL: String = "zipped_col"
  val OPTIMAL_TIME_REGION_COL: String = "optimal_time_region_index"

  val SECONDS_IN_A_DAY: Int = 24 * 3600

}
