import java.lang
import java.lang.String

import com.prajwalkk.hw2.MapReduceJobs.ContinuousAuthor
import com.prajwalkk.hw2.Utils.{ConfigUtils, XMLUtils}
import com.typesafe.config.Config
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.collection.immutable.List
import scala.xml.Elem

/*
*
* Created by: prajw
* Date: 22-Oct-20
*
*/
class TestConfigs extends AnyFunSuite {

  test("Test to check if configs are created") {
    val config = ConfigUtils.getConfigFile("mapredjobsTest.conf")
    config shouldBe a[Config]
    config.getString("Job1.name") shouldBe ("MapreduceJob1")
  }

  test("test to check XML tags for a sample"){
    val publicationString = """<inproceedings mdate="2017-05-24" key="conf/icst/GrechanikHB13"><author>Mark Grechanik</author><author>B. M. Mainul Hossain</author><author>Ugo Buy</author><title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title><pages>174-183</pages><year>2013</year><booktitle>ICST</booktitle><ee>https://doi.org/10.1109/ICST.2013.19</ee><ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee><crossref>conf/icst/2013</crossref><url>db/conf/icst/icst2013.html#GrechanikHB13</url></inproceedings>"""
    val xml = XMLUtils.createValidXML(publicationString)
    xml shouldBe a [Elem]

  }
  test("Checking if a publication element has proper tags"){
    val publicationString = """<inproceedings mdate="2017-05-24" key="conf/icst/GrechanikHB13"><author>Mark Grechanik</author><author>B. M. Mainul Hossain</author><author>Ugo Buy</author><title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title><pages>174-183</pages><year>2013</year><booktitle>ICST</booktitle><ee>https://doi.org/10.1109/ICST.2013.19</ee><ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee><crossref>conf/icst/2013</crossref><url>db/conf/icst/icst2013.html#GrechanikHB13</url></inproceedings>"""
    val xml = XMLUtils.createValidXML(publicationString)
    val authors = XMLUtils.extractAuthors(xml)
    authors should have length 3
    authors should contain allOf ("Mark Grechanik", "B. M. Mainul Hossain", "Ugo Buy")
  }

  test("Checking if a publication element has proper authors"){
    val publicationString = """<inproceedings mdate="2017-05-24" key="conf/icst/GrechanikHB13"><author>Mark Grechanik</author><author>B. M. Mainul Hossain</author><author>Ugo Buy</author><title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title><pages>174-183</pages><year>2013</year><booktitle>ICST</booktitle><ee>https://doi.org/10.1109/ICST.2013.19</ee><ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee><crossref>conf/icst/2013</crossref><url>db/conf/icst/icst2013.html#GrechanikHB13</url></inproceedings>"""
    val xml = XMLUtils.createValidXML(publicationString)
    val venues = XMLUtils.extractVenues(xml)
    venues shouldBe a [String]
    venues shouldBe "ICST"

  }

  test("Checking other year tags"){
    val publicationString = """<inproceedings mdate="2017-05-24" key="conf/icst/GrechanikHB13"><author>Mark Grechanik</author><author>B. M. Mainul Hossain</author><author>Ugo Buy</author><title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title><pages>174-183</pages><year>2013</year><booktitle>ICST</booktitle><ee>https://doi.org/10.1109/ICST.2013.19</ee><ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee><crossref>conf/icst/2013</crossref><url>db/conf/icst/icst2013.html#GrechanikHB13</url></inproceedings>"""
    val xml = XMLUtils.createValidXML(publicationString)
    val year = XMLUtils.extractYear(xml)
    year shouldBe "2013"
  }

  test("Checking other publication tags"){
    val publicationString = """<inproceedings mdate="2017-05-24" key="conf/icst/GrechanikHB13"><author>Mark Grechanik</author><author>B. M. Mainul Hossain</author><author>Ugo Buy</author><title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title><pages>174-183</pages><year>2013</year><booktitle>ICST</booktitle><ee>https://doi.org/10.1109/ICST.2013.19</ee><ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee><crossref>conf/icst/2013</crossref><url>db/conf/icst/icst2013.html#GrechanikHB13</url></inproceedings>"""
    val xml = XMLUtils.createValidXML(publicationString)
    val year = XMLUtils.extractPublicationName(xml)
    year shouldBe "Testing Database-Centric Applications for Causes of Database Deadlocks."
  }

}
