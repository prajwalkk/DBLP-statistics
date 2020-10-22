package com.prajwalkk.hw2.Utils


import com.typesafe.scalalogging.LazyLogging
import javax.xml.parsers.SAXParserFactory

import scala.xml.{Elem, XML}

/*
*
* Created by: prajw
* Date: 17-Oct-20
*
*/
object XMLUtils extends LazyLogging {
  private val xmlParser = SAXParserFactory.newInstance().newSAXParser()
  private val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI


  /**
   * Creates a DBLP XML using the SAX parser
   *
   * @param publicationString
   * @return
   */
  def createValidXML(publicationString: String): Elem = {
    val publicationStringCleaned = publicationString.replaceAll("&amp;", "&")
      .replaceAll("&apos;", "'")
      .replaceAll("&lt;", "<")
      .replaceAll("&gt;", ">")
      .replaceAll("[^\\x00-\\x7F]", "")
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dtdFilePath"><dblp>${publicationString}</dblp>"""
    logger.info(s"xmlString = $xmlString")
    XML.withSAXParser(xmlParser).loadString(xmlString)
  }

  /**
   * Extracts a list of authors
   *
   * @param str xml tag
   * @return List[String]
   */
  def extractAuthors(str: Elem): Seq[String] = {
    val authorTag = str.child.head.label match {
      case "book" | "proceedings" => "editor"
      case _ => "author"
    }
    val authors = (str \\ authorTag).collect(node => node.text)
    authors
  }

  /**
   * Extracts  venues from XML
   *
   * @param str
   * @return
   */
  def extractVenues(str: Elem): String = {
    // a venue will be the part of key in the appropriate tag
    // take the second part of the key example /a/ethos/ow09 -> return ethos
    val venueTag = str.child.head.label match {
      case "article" => "journal"
      case "inproceedings" | "proceedings" | "incollection" => "booktitle"
      case "book" | "phdthesis" | "mastersthesis" => "publisher"
      case _ => ""
    }
    if (venueTag == "") return ""
    val venue = if ((str \\ venueTag).text != "") (str \\ venueTag).text else "NoVenueSpecified"
    venue
  }

  /**
   * Extracts year from XML
   *
   * @param str
   * @return
   */
  def extractYear(str: Elem): String = {
    val yearTag = "year"
    val year = (str \\ yearTag).text
    year
  }

  /**
   * Extract publication title 
   *
   * @param str
   * @return
   */
  def extractPublicationName(str: Elem): String = {
    val titleTag = "title"
    val title = (str \\ titleTag).text
    title
  }
}
