package com.prajwalkk.hw2.Utils

import java.io.File


import com.typesafe.config.{Config, ConfigFactory}

import scala.jdk.CollectionConverters.CollectionHasAsScala


/*
*
* Created by: prajw
* Date: 15-Oct-20
*
*/
object ConfigUtils {
  val xmlDetails = ConfigFactory.load("xmltagpair.conf").getConfig("xmltags")

  def getXMLtags(string: String) =
    string.toLowerCase() match {
      case "start" => xmlDetails.getStringList("xml-start-tags").asScala.toList
      case "end" => xmlDetails.getStringList("xml-end-tags").asScala.toList
    }

  def getConfigFile(filePath: String): Config = {
    val mapredJobDetails = ConfigFactory.load(filePath).getConfig("mapred-pipeline")
    mapredJobDetails
  }
  def getJobTags(config:Config, str:String): Config = {
    config.getConfig(str)
  }
}
