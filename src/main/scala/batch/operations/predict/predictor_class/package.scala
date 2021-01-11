package batch.operations.predict

package object predictor_class {

  // Markovian predictor column names:
  object MarkovianPredictorConstFields {
    val TIMESTAMP_COL: String = "timestamp"
    val TEMPERATURE_COL: String = "temperature_value"
    val SOUND_VOLUME_COL: String = "sound_value"
    val DATE_COL: String = "date"
    val CURRENT_SECONDS_COL: String = "current_seconds"
    val PER_DAY_TIME_SERIES_COL: String = "mapped_col"
    val ZIPPED_TIME_VALUE_COL: String = "zipped_col"
    val ZIPPED_TIME_VALUE_LIST_COL: String = "zipped_list_col"
    val OPTIMAL_TIME_REGION_COL: String = "optimal_time_region_index"
  }

  // Mean predictor column names:
  val SECONDS_IN_A_DAY: Int = 24 * 3600
  object MeanPredictorConstFields {
    val TEMPERATURE_COL: String = "temperature_value"
    val HOUR_COL: String = "hour"
    val MEAN_NOISE_COL: String = "mean_noise"
    val TIMESTAMP_COL: String = "timestamp"
    val LAST_TIMESTAMP_COL: String = "last_timestamp"
    val SOUND_VOLUME_COL: String = "sound_value"
    val BEST_HOUR_COL: String = "best_hour"
    val BEST_HOUR_VALUE: String = "current_best_value"
    val MIN_TIMESTAMP: String = "min_timestamp"
    val MAX_TIMESTAMP: String = "max_timestamp"
    val DAYS_DIFFERENCE_COL: String = "days_difference"


  }

}
