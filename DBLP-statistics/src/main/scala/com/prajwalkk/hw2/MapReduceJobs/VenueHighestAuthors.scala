package com.prajwalkk.hw2.MapReduceJobs

import java.lang

import com.prajwalkk.hw2.Utils.XMLUtils.{createValidXML, extractAuthors, extractPublicationName, extractVenues}
import com.prajwalkk.hw2.Utils.{ConfigUtils, Constants}
import com.prajwalkk.hw2.parser.XMLInputFormat
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.jdk.CollectionConverters._

/*
*
* Created by: prajw
* Date: 18-Oct-20
*
*/
object VenueHighestAuthors extends LazyLogging {
  /**
   * Gets the Highest authors in each venue.
   * Mapper emite venue -> authors + publicationsting
   * Reducer emits venue -> highest publication
   *
   * @param configTypesafe
   */
  def runJob(configTypesafe: Config) = {

    val input: String = configTypesafe.getString(Constants.INPUT_PATH)
    val output: String = configTypesafe.getString(Constants.OUTPUT_PATH)
    val outputSeperator = configTypesafe.getString(Constants.SEPERATOR)

    logger.debug(s"${this.getClass}: Job initiated")
    val conf = new Configuration

    conf.set("mapreduce.output.textoutputformat.separator", outputSeperator)
    conf.set("mapreduce.map.log.level", "WARN")
    conf.set("mapreduce.reduce.log.level", "WARN")

    conf.setStrings(XMLInputFormat.START_TAG_KEY, ConfigUtils.getXMLtags(Constants.XML_START_TAG): _*)
    conf.setStrings(XMLInputFormat.END_TAG_KEY, ConfigUtils.getXMLtags(Constants.XML_END_TAG): _*)
    val jobName = configTypesafe.getString(Constants.NAME)
    val job = Job.getInstance(conf, jobName)
    job.setJarByClass(this.getClass)

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])

    job.setMapperClass(classOf[this.VenueMaxAuthMapper])
    job.setReducerClass(classOf[this.VenueMaxAuthReducer])

    job.setInputFormatClass(classOf[XMLInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])

    FileInputFormat.addInputPath(job, new Path(input))
    val outputDir = output + jobName + Path.SEPARATOR
    FileOutputFormat.setOutputPath(job, new Path(outputDir))

    System.exit(if (job.waitForCompletion(true)) 0
    else 1)
  }

  def convertListToMap(list: Iterable[String]): Map[String, Int] = {
    val mappedVals = list.map { tag =>
      (tag.split(">")(0), tag.split(">")(1).toInt)
    }.toMap
    mappedVals
  }

  // TODO create a custom input class for the mapper output Text(auth, Int)
  class VenueMaxAuthMapper extends Mapper[LongWritable, Text, Text, Text] {

    logger.debug(s"Mapper Initialized")
    private val venue = new Text()
    private val pubCount = new Text()

    override def map(key: LongWritable,
                     value: Text,
                     context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      logger.debug(s"Starting the map phase")
      val publicationElement = createValidXML(value.toString)
      val authors = extractAuthors(publicationElement)
      val publicationName = extractPublicationName(publicationElement)
      val venues = extractVenues(publicationElement)
      if (authors.nonEmpty && venues != "" && publicationName != "") {
        venue.set(venues)
        pubCount.set(publicationName + ">" + authors.size)
        logger.info(s"Mapper Emit: ${venues} -> $publicationName > ${authors.size}")
        context.write(venue, pubCount)
      }
    }
  }

  class VenueMaxAuthReducer extends Reducer[Text, Text, Text, Text] {
    logger.info("Reducer Initialized")

    override def reduce(key: Text,
                        values: lang.Iterable[Text],
                        context: Reducer[Text, Text, Text, Text]#Context): Unit = {

      val stringArray = values.asScala.map(value => value.toString)
      val authorCounts = convertListToMap(stringArray)
      // val (maxPublicaton, maxAuthorNumber) = authorCounts.maxBy(_._2)
      // Handles multiple max author. No TieBreaker needed.
      val maxMap = authorCounts.filter { case (k, v) =>
        v == authorCounts.values.max
      }
      val maxMapString = maxMap.map { case (k, v) =>
        s"$k ($v)"
      }.toList.mkString(",")
      logger.info(s"${key.toString} -> $maxMapString")
      context.write(key, new Text(maxMapString))
    }
  }

}
