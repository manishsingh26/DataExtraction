package DataAnalysis.ProductModules.Utils

import org.apache.spark.sql.{DataFrame, SparkSession}


class ElasticData(esURL: String) extends ConfigRead {

  val spark = SparkSession.builder().master("local[*]").appName("alice").config("hadoop.home.dir", "C:\\winutils").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def dataCheck(index: String, log_bundle: String, length: String): DataFrame = {
    val esQuery = """ {
                    |          "query": {
                    |            "match": {
                    |              "log_bundle": "log_bundle_name"
                    |            }
                    |          },
                    |          "size": "data_length"
                    |        } """.replaceFirst("log_bundle_name", log_bundle).replaceFirst("data_length", length)
    val indexPath = index + "/log"
    val df = spark.read.format("org.elasticsearch.spark.sql")
                      .option("es.node", esURL)
                      .option("query", esQuery)
                      .option("pushdown", "true")
                      .load(indexPath)
    df
  }

  def dataPush(file_path: String, index_name: String): String= {
    val df = spark.read.option("header","true").csv(file_path)
    val indexPath = esURL + "/log"
    df.write.format("org.elasticsearch.spark.sql")
            .option("es.node", esURL)
            .mode("Overwrite")
            .save(indexPath)
    "Done"
  }

  def readConfig(configType: String, configIndex: String): DataFrame= {

    val configKeyType = configType
    val configDF = configKeyType match {
      case "pattern"    =>      readPatternConfig(configIndex, spark, esURL)
      case "rule"       =>      readRuleConfig(configIndex, spark, esURL)
      case _            =>      throw new IllegalArgumentException
    }
    configDF
  }
}

