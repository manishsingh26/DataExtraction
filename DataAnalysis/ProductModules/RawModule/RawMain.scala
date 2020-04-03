package DataAnalysis.ProductModules.RawModule


import java.io.File
import scala.io.Source
import scala.sys.process._
import com.typesafe.config.Config
import java.nio.file.{FileSystems, Files, Path}
import java.util.regex.Pattern
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._


object RawMain {
  System.setProperty("hadoop.home.dir", "C:\\winutils")
  def extractExpressedFile(appConfig: Config): scala.List[Path] = {

    val logBundlePath = appConfig.getConfig("user_details").getString("log_bundle_path")
    val extractedLogBundlePath = appConfig.getConfig("user_details").getString("extracted_log_bundle_path")
    val recursivePythonScript = appConfig.getConfig("python_script").getConfig("recursive_extraction").getString("actual_path")

    def pythonRecursiveExtraction(recursivePythonScript: String, logBundlePath: String, extractedLogBundlePath: String): Unit = {

      val pythonRecursiveExtractionPath = Seq("python", recursivePythonScript, logBundlePath, extractedLogBundlePath)
      val triggerRecursiveScript = Process(pythonRecursiveExtractionPath).lineStream.toList.head.toString

      if (triggerRecursiveScript == "Done"){
        println("Zip extracted successfully.")
      } else {
        println("Error in zip extraction.")
      }
    }
    pythonRecursiveExtraction(recursivePythonScript, logBundlePath, extractedLogBundlePath)

    def findAllFilesPath(extractedLogBundlePathDir: String): scala.List[Path] = {

      val logBundleDirectory = FileSystems.getDefault.getPath(extractedLogBundlePathDir)
      val allFilesList = Files.walk(logBundleDirectory).iterator().asScala.filter(Files.isRegularFile(_)).toList
      allFilesList
    }
    val allFilesList = findAllFilesPath(extractedLogBundlePath)
    allFilesList
  }

  def rawLogParser(allFilesList: scala.List[Path], appConfig: Config): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("alice").config("hadoop.home.dir", "C:\\winutils").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val userName = appConfig.getConfig("user_details").getString("user_name")
    val uploadTime = appConfig.getConfig("user_details").getString("upload_time")
    val product = appConfig.getConfig("user_details").getString("product")
    val logBundleName = appConfig.getConfig("user_details").getString("log_bundle_name")
    var statusDataSeq = List[List[String]]()

    for (eachPath <- allFilesList) {

      val eachPathArray = eachPath.toString.split(Pattern.quote(File.separator)).toList
      val fileIndex = eachPathArray.length - 1
      val fileName = eachPathArray(fileIndex)
      val folderIndex = eachPathArray.length - 2
      val folderName = eachPathArray(folderIndex)

      var rawDataSeq = Seq[Seq[String]]()
      val fileReadObject = Source.fromFile(eachPath.toString).getLines()
      val fileTotalRows = fileReadObject.length.toString
      val startTimeMillis = System.currentTimeMillis()

      try {
        for ((line, index) <- Source.fromFile(eachPath.toString).getLines().zipWithIndex) {
          val indexFinal = (index + 1).toString
          val indexLine = line.toString
          val finalRawDataArray = Seq(indexFinal, userName, uploadTime, product, logBundleName, folderName, fileName, indexLine)
          rawDataSeq = rawDataSeq :+ finalRawDataArray
        }
        import spark.sqlContext.implicits._
        val mapRawColumns = rawDataSeq.map { case Seq(a: String, b: String, c: String, d: String, e: String, f: String, g: String, h: String) => (a, b, c, d, e, f, g, h) }
        val rawDF = spark.sparkContext.parallelize(mapRawColumns).toDF("row_index", "username", "timestamp", "product", "log_bundle", "folder_name", "file_name", "log_text")
//        println(rawDF.show(5))

        val endTimeMillis = System.currentTimeMillis()
        val timeDifferenceMilliSecond = (endTimeMillis - startTimeMillis).toString
        val finalStatusDataArray = List(userName, uploadTime, product, logBundleName, folderName, fileName, timeDifferenceMilliSecond, fileTotalRows, "Done")
        statusDataSeq = statusDataSeq :+ finalStatusDataArray

      } catch {
        case error: Exception =>
          val endTimeMillis = System.currentTimeMillis()
          val timeDifferenceMilliSecond = (endTimeMillis - startTimeMillis).toString
          val finalStatusDataArray = List(userName, uploadTime, product, logBundleName, folderName, fileName, timeDifferenceMilliSecond, fileTotalRows, error.toString)
          statusDataSeq = statusDataSeq :+ finalStatusDataArray

      }
    }
//    for (eachStatus <- statusDataSeq){
//      println(eachStatus)
//    }

  }
}
