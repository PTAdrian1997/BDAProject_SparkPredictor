import com.typesafe.config.{Config, ConfigFactory}

package object application {

  val foundingConfig: Config = ConfigFactory.parseResources("run_configuration.conf")

  lazy val readOperatorConfig: Config = foundingConfig.getConfig("ReadOperator")
  lazy val writeOperatorConfig: Config = foundingConfig.getConfig("WriteOperator")
  lazy val predictOperatorConfig: Config = foundingConfig.getConfig("PredictOperator")

  case class ReadOperatorConfig(
                               url: String = readOperatorConfig.getString("connection_url"),
                               inputTable: String = readOperatorConfig.getString("input_table"),
                               inputDatabase: String = readOperatorConfig.getString("input_database"),
                               dataOrigin: String = readOperatorConfig.getString("data_origin")
                               )
  lazy val readOperatorConfigElements: ReadOperatorConfig = ReadOperatorConfig()

  case class MarkovianPredictorConfig(configuration: Config){
    val predictorGranularity: Int = configuration.getInt("predictor_granularity")
    val dataAgeInfluence: Int = configuration.getInt("data_age_influence")
  }

  case class PredictOperatorConfig(
                                    predictorTypeString: String = predictOperatorConfig
                                      .getString("predictor_type"),
                                    stochasticPredictorConfig: MarkovianPredictorConfig = MarkovianPredictorConfig(
                                      predictOperatorConfig.getConfig("MarkovianPredictor"))
                                  )
  lazy val predictOperatorConfigElements: PredictOperatorConfig = PredictOperatorConfig()

  case class WriteOperatorConfig(outputPath: String = writeOperatorConfig.getString("output_path"))
  lazy val writeOperatorConfigElements: WriteOperatorConfig = WriteOperatorConfig()

}
