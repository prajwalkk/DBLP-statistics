package com.prajwalkk.hw2.MapReduceJobs

import java.lang

import com.prajwalkk.hw2.Utils.{ConfigUtils, Constants}
import com.prajwalkk.hw2.Utils.XMLUtils._
import com.prajwalkk.hw2.parser.XMLInputFormat
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.jdk.CollectionConverters.IterableHasAsScala

/*
*
* Created by: prajw
* Date: 17-Oct-20
*
*/
object AuthorVenueRank extends LazyLogging {

  /**
   * Job runner
   * This Job generates the to 10 authors in each venue. Each ordered by the number of times they
   * present their works in the same venue
   *
   * @param configTypesafe Job config
   */
  def runJob(configTypesafe: Config) = {

    val input: String = configTypesafe.getString(Constants.INPUT_PATH)
    val output: String = configTypesafe.getString(Constants.OUTPUT_PATH)
    val outputSeperator = configTypesafe.getString(Constants.SEPERATOR)

    val conf = new Configuration
    conf.set("mapreduce.output.textoutputformat.separator", outputSeperator)

    conf.setStrings(XMLInputFormat.START_TAG_KEY, ConfigUtils.getXMLtags(Constants.XML_START_TAG): _*)
    conf.setStrings(XMLInputFormat.END_TAG_KEY, ConfigUtils.getXMLtags(Constants.XML_END_TAG): _*)

    val jobName: String = configTypesafe.getString(Constants.NAME)
    val job = Job.getInstance(conf, jobName)
    job.setJarByClass(this.getClass)

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])

    job.setMapperClass(classOf[this.VenueAuthMap])
    job.setReducerClass(classOf[this.RankReducer])

    job.setInputFormatClass(classOf[XMLInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])

    FileInputFormat.addInputPath(job, new Path(input))
    val outputDir = output + jobName + Path.SEPARATOR
    FileOutputFormat.setOutputPath(job, new Path(outputDir))

    System.exit(if (job.waitForCompletion(true)) 0
    else 1)
  }

  /**
   * Takes in Text and emits venue -> author
   *
   */
  class VenueAuthMap extends Mapper[Object, Text, Text, Text] {
    logger.debug(s"${this.getClass}: Mapper phase Initated")
    private val venueKey = new Text()
    private val authorValue = new Text()

    override def map(key: Object,
                     value: Text,
                     context: Mapper[Object, Text, Text, Text]#Context): Unit = {

      // Get single XML tag
      logger.debug(s"${this.getClass}: map phase started")
      val publicationElement = createValidXML(value.toString)
      // get authors
      val authors = extractAuthors(publicationElement)
      // get venues for that publication
      val venues = extractVenues(publicationElement)
      if (authors.nonEmpty && venues != "") {
        // venue is at most one. So take the first element and map all the authors to that venue
        authors.foreach { author =>
          // use a good delimiter
          val intString = s"$venues(0) --> $author"
          venueKey.set(venues)
          authorValue.set(author)
          logger.info(s"Mapper Emit: $intString , 1")
          context.write(venueKey, authorValue)
        }
      }
    }
  }

  /**
   * Generates K, V pairs of venue -> top10 authors(frequncy)
   *
   */
  class RankReducer extends Reducer[Text, Text, Text, Text] {
    logger.debug(s"${this.getClass}: Reducer phase initaited")
    private val result = new Text

    override def reduce(key: Text,
                        values: lang.Iterable[Text],
                        context: Reducer[Text, Text, Text, Text]#Context): Unit = {

      logger.debug(s"${this.getClass}: Reducer phase started")
      val authors = values.asScala.map(value => value.toString).toList
      // Generates reverse ordering of (author ) based off frequency. Sliced to take only 10 values.
      if (authors.nonEmpty) {
        val topTen = authors.groupBy(identity).toSeq
          .sortBy(-_._2.size)
          .map {
            case (auth, freqs) => (s"$auth(${freqs.size})")
            case _ => ""
          }.slice(0, 10)
        // We want top-10 to be as a one line string
        if (topTen.nonEmpty) {
          val singleLineText = topTen.mkString(",")
          logger.info(s"Reducer Emit: ${key.toString} -> $singleLineText")
          result.set(singleLineText)
          context.write(key, result)
        }
      }
    }
  }


}
