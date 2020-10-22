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

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.sys.process.stringSeqToProcess


/*
*
* Created by: prajw
* Date: 18-Oct-20
*
*/
object VenueSingleAuthor extends LazyLogging {

  /**
   * This job has the map phase that puts venue -> publication whose author is 1
   * th reduces collects all the publications in that venue and emits them as a single string concatenated by ,
   *
   * @param configTypesafe
   */
  def runJob(input: String, output: String, configTypesafe: Config) = {

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

    job.setMapperClass(classOf[this.VenueSingleAuthor])
    job.setReducerClass(classOf[this.VenuePublicationReduce])

    job.setInputFormatClass(classOf[XMLInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])

    FileInputFormat.addInputPath(job, new Path(input))
    val outputDir = output + jobName + Path.SEPARATOR
    val outputPath = new Path(outputDir)
    outputPath.getFileSystem(conf).delete(outputPath, true)
    FileOutputFormat.setOutputPath(job, outputPath)

    if (job.waitForCompletion(true)) {
      logger.info(s"Success on ${jobName}")
      val a = Seq("hdfs", "dfs", "-getmerge", s"${output}${jobName}/*", "./SingleAuthorsPerVenue_Job3.csv").!!
      val b = Seq("hdfs", "dfs", "-mkdir", "-p", outputDir+"FinalOP/").!!
      val c = Seq("hdfs", "dfs", "-put" ,"./SingleAuthorsPerVenue_Job3.csv", outputDir+"FinalOP/").!!
      logger.info(s"Status $a $b $c")
    }
    else logger.error(s"Failed on ${jobName}")
  }


  class VenueSingleAuthor extends Mapper[LongWritable, Text, Text, Text] {

    logger.info(s"${this.getClass} Running Map job")
    private val venue = new Text()


    override def map(key: LongWritable,
                     value: Text,
                     context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      logger.debug(s"${this.getClass} Starting the map phase")
      // Get single XML snippet
      val publicationElement = createValidXML(value.toString)
      // extract authors, publication names, venues
      val authors = extractAuthors(publicationElement)
      val publicationName = extractPublicationName(publicationElement)
      val venues = extractVenues(publicationElement)
      // filter single author
      if (authors.size == 1 && venues != "" && publicationName != "") {
        venue.set(venues)
        logger.info(s"Mapper Emit$venues -> $publicationName")
        context.write(venue, new Text(publicationName))
      }
    }
  }

  class VenuePublicationReduce extends Reducer[Text, Text, Text, Text] {
    logger.debug(s"${this.getClass} Initializing the reduce phase")

    override def reduce(key: Text,
                        values: lang.Iterable[Text],
                        context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      logger.debug(s"${this.getClass} Starting the reduce phase")
      val stringArray = values.asScala.map(value => value.toString).toList
      // Add a delimiter between publications
      val singleAuthorString = stringArray.mkString("||")
      logger.info(s"Mapper Emit ${key.toString} -> $singleAuthorString")
      context.write(key, new Text(singleAuthorString))
    }
  }

}
