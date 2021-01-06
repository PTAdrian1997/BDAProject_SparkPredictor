package batch

import java.sql.Timestamp

package object operations {

  case class InputRecord(timestamp: Timestamp, temperature_value: Double, sound_value: Double)

}
