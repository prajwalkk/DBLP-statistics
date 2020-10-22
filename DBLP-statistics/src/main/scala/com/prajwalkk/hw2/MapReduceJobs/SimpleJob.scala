package com.prajwalkk.hw2.MapReduceJobs

import java.lang
import java.util.concurrent.atomic.AtomicInteger

import com.prajwalkk.hw2.Utils.ConfigUtils
import com.prajwalkk.hw2.parser.XMLInputFormat
import com.typesafe.scalalogging.LazyLogging
import javax.xml.parsers.SAXParserFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.xml.{Elem, XML}

/*
*
* Created by: prajw
* Date: 13-Oct-20
*
*/
object SimpleJob extends LazyLogging {


  @throws[Exception]
  def runJob(input: String, output: String): Unit = {
    val conf = new Configuration
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    conf.setStrings(XMLInputFormat.START_TAG_KEY, ConfigUtils.getXMLtags("start"): _*)
    conf.setStrings(XMLInputFormat.END_TAG_KEY, ConfigUtils.getXMLtags("end"): _*)
    val jobName = "MapReduceJob1"
    val job = Job.getInstance(conf, jobName)
    job.setJarByClass(this.getClass)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setMapperClass(classOf[SimpleJob.Map])
    job.setReducerClass(classOf[SimpleJob.Reduce])
    job.setInputFormatClass(classOf[XMLInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
    FileInputFormat.addInputPath(job, new Path(input))
    val outputDir = output.replace("(jobName)", jobName)
    FileOutputFormat.setOutputPath(job, new Path(outputDir))
    if (job.waitForCompletion(true))
      logger.info(s"Success on ${jobName}")
    else
      logger.error(s"Fail on ${jobName}")
  }


  class Map extends Mapper[LongWritable, Text, Text, IntWritable] {
    private val xmlParser = SAXParserFactory.newInstance().newSAXParser()
    private val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI

    private val one = new IntWritable(1)
    private val authorKey = new Text()

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      logger.debug(s"Starting the map phase for $key -> ${value.toString}")


      val publicationElement = createValidXML(value.toString)
      val authors = extractAuthors(publicationElement)
      if (authors.nonEmpty)
        authors.foreach { author =>
          authorKey.set(author)
          logger.info(s"Mapper Emit $author, 1")
          context.write(authorKey, one)
        }
    }

    def createValidXML(publicationString: String): Elem = {
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

      val authors = (str \\ authorTag).collect({ case node => node.text })
      logger.info(s"Authors of XML: $authors")
      authors
    }
  }

  class Reduce extends Reducer[Text, IntWritable, Text, IntWritable] {

    private val result = new IntWritable

    override def reduce(key: Text,
                        values: lang.Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

      val sum = new AtomicInteger(0)
      values.asScala.foreach(value => sum.addAndGet(value.get))
      result.set(sum.get())
      context.write(key, result)
    }
  }

}
