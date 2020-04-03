package DataAnalysis.ProductModules


import java.io.File
import java.nio.file.Path

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import DataAnalysis.ProductModules.RawModule.RawMain
import DataAnalysis.ProductModules.PatternModule._

class Module(userName: String, uploadTime: String, product: String, logBundle: String) {

  def moduleDependencies(): Config = {
    val confFilePath = new File("C:\\Users\\m4singh\\IdeaProjects\\Alice\\src\\main\\scala\\resources\\application.conf")
    val appConfig = ConfigFactory.parseFile(confFilePath)
    val rootPath = "C:\\Users\\m4singh\\IdeaProjects\\Alice"
    val detailsSeparator = appConfig.getConfig("system_details").getString("details_separator")
    val logBundleDir = appConfig.getConfig("data_flow").getString("input")
    val genericTempDir = appConfig.getConfig("data_flow").getConfig("generic_temp").getString("user_temp")
    val genericRawTempDir = appConfig.getConfig("data_flow").getConfig("generic_temp").getConfig("sub_modules_path").getString("raw")
    val genericPatternTempDir = appConfig.getConfig("data_flow").getConfig("generic_temp").getConfig("sub_modules_path").getString("pattern")
    val genericRuleTempDir = appConfig.getConfig("data_flow").getConfig("generic_temp").getConfig("sub_modules_path").getString("rule")
    val logBundleSepArray = logBundle.split(detailsSeparator)
    val logBundlePath = rootPath + File.separator + logBundleDir + File.separator + logBundle
    val logBundleName = logBundleSepArray(0)

    val recursivePythonPath = appConfig.getConfig("python_script").getConfig("recursive_extraction").getString("project_path")
    val actualRecursivePythonPython = rootPath + File.separator + recursivePythonPath

    val appConfigRecursivePython = appConfig.withValue("python_script.recursive_extraction.actual_path", ConfigValueFactory.fromAnyRef(actualRecursivePythonPython))
    val appConfUserName = appConfigRecursivePython.withValue("user_details.user_name", ConfigValueFactory.fromAnyRef(userName))
    val appConfUploadTime = appConfUserName.withValue("user_details.upload_time", ConfigValueFactory.fromAnyRef(uploadTime))
    val appConfProduct = appConfUploadTime.withValue("user_details.product", ConfigValueFactory.fromAnyRef(product))
    val appConfLogBundle = appConfProduct.withValue("user_details.log_bundle", ConfigValueFactory.fromAnyRef(logBundle))
    val appConfLogBundlePath = appConfLogBundle.withValue("user_details.log_bundle_path", ConfigValueFactory.fromAnyRef(logBundlePath))
    val appConfLogBundleName = appConfLogBundlePath.withValue("user_details.log_bundle_name", ConfigValueFactory.fromAnyRef(logBundleName))
    val appConfUserInformation = appConfLogBundleName.withValue("root_path", ConfigValueFactory.fromAnyRef(rootPath))

    val userTempDumpName = userName + detailsSeparator + uploadTime  + detailsSeparator + product + detailsSeparator + logBundleName
    val userTempDumpPath = rootPath + File.separator +  genericTempDir + File.separator + userTempDumpName
    new File(userTempDumpPath).mkdirs

    val userRawTempDumpPath = rootPath + File.separator +  genericTempDir + File.separator + userTempDumpName + File.separator + genericRawTempDir
    new File(userRawTempDumpPath).mkdirs
    val appConfRawTemp = appConfUserInformation.withValue("generic_module.module_1.temp_details.raw_data_path", ConfigValueFactory.fromAnyRef(userRawTempDumpPath))

    val userPatternTempDumpPath = rootPath + File.separator +  genericTempDir + File.separator  + userTempDumpName + File.separator + genericPatternTempDir
    new File(userPatternTempDumpPath).mkdirs
    val appConfigPatternTemp = appConfRawTemp.withValue("generic_module.module_2.temp_details.pattern_data_path", ConfigValueFactory.fromAnyRef(userRawTempDumpPath))

    val userRuleTempDumpPath = rootPath + File.separator +  genericTempDir + File.separator + userTempDumpName + File.separator + genericRuleTempDir
    new File(userRuleTempDumpPath).mkdirs
    val appConfigRuleTemp = appConfigPatternTemp.withValue("generic_module.module_3.temp_details.rule_data_path", ConfigValueFactory.fromAnyRef(userRuleTempDumpPath))

    val extractedLogBundlePath = rootPath + File.separator + logBundleDir + File.separator + userTempDumpName
    new File(extractedLogBundlePath).mkdirs
    val appConfigFinal = appConfigRuleTemp.withValue("user_details.extracted_log_bundle_path", ConfigValueFactory.fromAnyRef(extractedLogBundlePath))

    appConfigFinal
  }

  def raw_module_trigger(appConfig: Config): scala.List[Path] = {
    val rawMainObj = RawMain
    val allFilesList = rawMainObj.extractExpressedFile(appConfig)
    rawMainObj.rawLogParser(allFilesList, appConfig)
    allFilesList
  }

  def pattern_module_trigger(appConfig: Config, allFilesList: scala.List[Path]): Unit = {
    val patternObj = new PatternMain(appConfig, allFilesList)
    val productDF = patternObj.configExtraction()
    patternObj.patternExtraction(productDF)


  }
}
