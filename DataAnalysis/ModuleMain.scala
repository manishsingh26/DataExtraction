package DataAnalysis


import DataAnalysis.ProductModules._
import Utils._

object ModuleMain {
  def main(args: Array[String]): Unit = {

    val userName = "m4singh"
    val uploadTime = "09-05-2019@13-36-12.082"
    val product = "Flexi-BSC"
    val logBundle = "ping###m4singh###09-05-2019@13-36-12.082.zip"

    val genericModuleObj = new Module(userName, uploadTime, product, logBundle)
    val appConfig = genericModuleObj.moduleDependencies()
    val allFilesList = genericModuleObj.raw_module_trigger(appConfig)
    genericModuleObj.pattern_module_trigger(appConfig, allFilesList)

  }
}
