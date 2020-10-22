package com.prajwalkk.hw2.Utils


import javax.xml.parsers.SAXParserFactory

import scala.xml.{Elem, XML}

/*
*
* Created by: prajw
* Date: 17-Oct-20
*
*/
object XMLUtils {
  private val xmlParser = SAXParserFactory.newInstance().newSAXParser()
  private val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI


  def createValidXML(publicationString: String): Elem = {
    val publicationStringCleaned = publicationString.replaceAll("&amp;", "&")
      .replaceAll("&apos;", "'")
      .replaceAll("&lt;", "<")
      .replaceAll("&gt;", ">")
      .replaceAll("[^\\x00-\\x7F]", "")
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dtdFilePath"><dblp>${publicationString}</dblp>"""
    println(s"xmlString = $xmlString")
    XML.withSAXParser(xmlParser).loadString(xmlString)
  }

  def extractAuthors(str: Elem): Seq[String] = {
    val authorTag = str.child.head.label match {
      case "book" | "proceedings" => "editor"
      case _ => "author"
    }
    val authors = (str \\ authorTag).collect(node => node.text)
    authors
  }

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

  def extractYear(str: Elem): String = {
    val yearTag = "year"
    val year = (str \\ yearTag).text
    year
  }

  def extractPublicationName(str: Elem): String = {
    val titleTag = "title"
    val title = (str \\ titleTag).text
    title
  }
}
