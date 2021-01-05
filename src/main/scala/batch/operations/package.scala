package batch

import java.sql.Timestamp

package object operations {

  case class InputRecord(timestamp: Timestamp, temperatureValue: Double, soundValue: Double)

}
