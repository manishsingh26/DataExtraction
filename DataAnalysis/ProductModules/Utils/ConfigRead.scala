package DataAnalysis.ProductModules.Utils

import org.apache.spark.sql.{DataFrame, SparkSession}

trait ConfigRead {


  def readPatternConfig(configIndex: String, spark: SparkSession, esURL: String): DataFrame= {
    val esQuery = """{"query" : {"match_all" : {}}}"""
    val configIndexPath = configIndex + "/log"
    val patternDF = spark.read.format("org.elasticsearch.spark.sql")
                        .option("es.node", esURL)
                        .option("query", esQuery)
                        .option("pushdown", "true")
                        .load(configIndexPath)
    patternDF
  }

  def readRuleConfig(configIndex: String, spark: SparkSession, esURL: String): DataFrame= {
    val esQuery = """{"query" : {"match_all" : {}}}"""
    val configIndexPath = configIndex + "/log"
    val ruleDF = spark.read.format("org.elasticsearch.spark.sql")
                        .option("es.node", esURL)
                        .option("query", esQuery)
                        .option("pushdown", "true")
                        .load(configIndexPath)
    ruleDF
  }
}
