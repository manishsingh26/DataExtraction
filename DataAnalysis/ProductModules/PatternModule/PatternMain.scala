package DataAnalysis.ProductModules.PatternModule

import java.nio.file.Path

import scala.collection.mutable
import scala.collection.JavaConversions._
import com.typesafe.config.Config
import DataAnalysis.ProductModules.Utils._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


class PatternMain(appConfig: Config, allFilesList:scala.List[Path]) {

  val spark = SparkSession.builder().master("local[*]").appName("alice").config("hadoop.home.dir", "C:\\winutils").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def configExtraction(): DataFrame = {
    val esLink = appConfig.getConfig("system_details").getConfig("elastic_details").getString("host")
    val configType = appConfig.getConfig("generic_module").getConfig("pattern_module").getString("config_keyword")
    val configIndex = appConfig.getConfig("generic_module").getConfig("pattern_module").getConfig("es_details").getString("config_pattern_index")
    val product = appConfig.getConfig("user_details").getString("product")

    val esObj = new ElasticData(esLink)
    val df = esObj.readConfig(configType, configIndex)
    val productDF = df.filter(df("product")===product && df("pattern_level")==="Global")
    productDF
  }

  def patternExtraction(productDF: DataFrame): Unit = {

    val rowsRdd = spark.sparkContext.emptyRDD[Row]
    val baseSchema = StructType(Seq(StructField("rel_version", StringType, nullable = true), StructField("folder", StringType, nullable = true),
      StructField("file_name", StringType, nullable = true), StructField("pattern_name", StringType, nullable = true), StructField("product", StringType, nullable = true),
      StructField("username", StringType, nullable = true), StructField("log_bundle", StringType, nullable = true), StructField("index_timestamp", StringType, nullable = true),
      StructField("pattern_seq_id", StringType, nullable = true), StructField("unique_pattern_key", StringType, nullable = true), StructField("schema_file_name", StringType, nullable = true),
      StructField("date_time", StringType, nullable = true), StructField("key1", StringType, nullable = true), StructField("key2", StringType, nullable = true),
      StructField("key3", StringType, nullable = true), StructField("key4", StringType, nullable = true), StructField("key5", StringType, nullable = true),
      StructField("key6", StringType, nullable = true), StructField("key7", StringType, nullable = true), StructField("key8", StringType, nullable = true),
      StructField("key9", StringType, nullable = true), StructField("key10", StringType, nullable = true), StructField("val1", StringType, nullable = true),
      StructField("val2", StringType, nullable = true), StructField("val3", StringType, nullable = true), StructField("val4", StringType, nullable = true),
      StructField("val5", StringType, nullable = true), StructField("val5", StringType, nullable = true), StructField("val6", StringType, nullable = true),
      StructField("val7", StringType, nullable = true), StructField("val8", StringType, nullable = true), StructField("val9", StringType, nullable = true),
      StructField("val10", StringType, nullable = true)))
    val appendDF = spark.createDataFrame(rowsRdd, baseSchema)

    def patternCheck(eachPath: String): ListBuffer[ListBuffer[(String, String, String)]] = {

      var statusList = ArrayBuffer[(String, String, String)]
      spark.sparkContext.broadcast(productDF)
      productDF.foreach { eachRow =>


        val patternFolder = eachRow(eachRow.fieldIndex("folder"))
        val patternFile = eachRow(eachRow.fieldIndex("file_name"))
        val patternSchema = eachRow(eachRow.fieldIndex("pattern_schema"))
        val patternName = eachRow(eachRow.fieldIndex("pattern_name"))
        val enableStatus = eachRow(eachRow.fieldIndex("enabled"))
        val patternLevel = eachRow(eachRow.fieldIndex("pattern_level"))

        val eachData = if (eachPath.toString.toLowerCase().contains(patternFolder.toString.toLowerCase()) & eachPath.toString.toLowerCase().contains(patternFile.toString.toLowerCase())) {
          println((eachPath, patternFolder.toString, patternFile.toString))
          (eachPath, patternFolder.toString, patternFile.toString)
        }.
        statusList += eachData
      }
      println(statusList.toString())
      statusList    }

    for (eachPath <- allFilesList) {
      println(eachPath)
      val eachStatus = patternCheck(eachPath.toString)

//      for (k <- eachStatus){
//        println(k)
//        println("_______")
//      }
//      println("************")
    }
  }
}
