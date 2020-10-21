package com.prajwalkk.hw2.Utils

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
  val mapredJobDetails = ConfigFactory.load("mapredjobs.conf").getConfig("mapred-pipeline")

  def getXMLtags(string: String) =
    string.toLowerCase() match {
      case "start" => xmlDetails.getStringList("xml-start-tags").asScala.toList
      case "end" => xmlDetails.getStringList("xml-end-tags").asScala.toList
    }

  def getJobTags(str: String): Config = {
    mapredJobDetails.getConfig(str)
  }
}
