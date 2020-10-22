package com.prajwalkk.hw2.MapReduceJobs

import java.lang

import com.prajwalkk.hw2.Utils.XMLUtils._
import com.prajwalkk.hw2.Utils.{ConfigUtils, Constants}
import com.prajwalkk.hw2.parser.XMLInputFormat
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.sys.process.stringSeqToProcess

/*
*
* Created by: prajw
* Date: 18-Oct-20
*
*/
object ContinuousAuthor extends LazyLogging {


  /**
   * This job has mapper emitting Author -> year.
   * The reducers calculates the max continuous year range for that author
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
    conf.setInt(Constants.YEAR_NUM, configTypesafe.getString(Constants.YEAR_NUM).toInt)
    val jobName = configTypesafe.getString(Constants.NAME)
    val job = Job.getInstance(conf, jobName)
    job.setJarByClass(this.getClass)

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])

    job.setMapperClass(classOf[this.AuthorYearMap])
    job.setReducerClass(classOf[this.AuthorYearReduce])

    job.setInputFormatClass(classOf[XMLInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])

    FileInputFormat.addInputPath(job, new Path(input))
    val outputDir = output + jobName + Path.SEPARATOR
    val outputPath = new Path(outputDir)
    outputPath.getFileSystem(conf).delete(outputPath, true)
    FileOutputFormat.setOutputPath(job, outputPath)

    if (job.waitForCompletion(true)) {
      logger.info(s"Success on ${jobName}")
      try {
        Seq("hdfs", "dfs", "-getmerge", s"$outputDir*", "./continuous_n_author_job2.csv").!!
        Seq("hdfs", "dfs", "-mkdir", "-p", outputDir + "FinalOP/").!!
        Seq("hdfs", "dfs", "-put", "./continuous_n_author_job2.csv", outputDir + "FinalOP/").!!
      } catch {
        case _ : Throwable => logger.error("Something wrong with writing of final files. Do it yourself.")
      }

    }
    else logger.error(s"Failed on ${jobName}")
  }

  class AuthorYearMap extends Mapper[LongWritable, Text, Text, IntWritable] {
    logger.debug(s"${this.getClass}: Mapper Job Started")
    private val authorKey = new Text()
    private val yearVal = new IntWritable()

    override def map(key: LongWritable,
                     value: Text,
                     context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      logger.debug(s"${this.getClass}: Mapper Job initiated")
      val publicationElement = createValidXML(value.toString)
      val authors = extractAuthors(publicationElement)
      val year = extractYear(publicationElement)
      if (authors.nonEmpty && year != "") {
        authors.foreach { author =>
          authorKey.set(author)
          yearVal.set(Integer.parseInt(year))
          logger.info(s"Mapper Emit $author -> Year: $year")
          context.write(authorKey, yearVal)
        }
      }
    }
  }

  class AuthorYearReduce extends Reducer[Text, IntWritable, Text, IntWritable] {

    private val result = new IntWritable

    override def reduce(key: Text,
                        values: lang.Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

      logger.debug(s"${this.getClass}: Reducer initiated")
      val yearRange = context.getConfiguration.getInt(Constants.YEAR_NUM, 10)
      val valuesAsInt = values.asScala.map(value => value.get).toList
      val maxRange = if (valuesAsInt.nonEmpty) findMaxRange(valuesAsInt) else 0
      logger.info(s"Reducer emit: ${key.toString} -> $maxRange")
      if (maxRange >= yearRange) {
        context.write(key, new IntWritable(maxRange))
      }
      
    }

    // credits @link{https://stackoverflow.com/questions/36778213/how-to-find-the-maximum-consecutive-years-for-each-id-using-scala-spark}
    def findMaxRange(years: List[Int]): Int = {
      // initial range value
      val ranges = ArrayBuffer[Int](1)
      years.sorted.distinct.sliding(2).foreach { case y1 :: tail =>
        if (tail.nonEmpty) {
          val y2 = tail.head
          if (y2 - y1 == 1) ranges(ranges.size - 1) += 1
          else ranges += 1
        }
      // handled null arrays above. this is to eliminate warnings of compiler
      case _ => 0
      }
      ranges.max
    }
  }

}
