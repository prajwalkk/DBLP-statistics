package com.prajwalkk.hw2.MapReduceJobs

import java.lang
import java.util.concurrent.atomic.AtomicInteger

import com.prajwalkk.hw2.Utils.ConfigUtils
import com.prajwalkk.hw2.parser.XmlInputFormat
import com.typesafe.scalalogging.LazyLogging
import javax.xml.parsers.SAXParserFactory

import scala.xml.{Elem, XML}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.sys.exit

/*
*
* Created by: prajw
* Date: 13-Oct-20
*
*/
object JobDriver extends LazyLogging {


  def main(args: Array[String]): Unit = {
    runJob(args(0), args(1))
  }

  @throws[Exception]
  def runJob(input: String, output: String): Unit = {
    val conf = new Configuration
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    conf.setStrings(XmlInputFormat.START_TAG_KEY, ConfigUtils.getXMLtags("start"): _*)
    conf.setStrings(XmlInputFormat.END_TAG_KEY, ConfigUtils.getXMLtags("end"): _*)
    val jobName = "MapReduceJob1"
    val job = Job.getInstance(conf, jobName)
    job.setJarByClass(this.getClass)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setMapperClass(classOf[JobDriver.Map])
    job.setReducerClass(classOf[JobDriver.Reduce])
    job.setInputFormatClass(classOf[XmlInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
    FileInputFormat.addInputPath(job, new Path(input))
    val outputDir = output.replace("(jobName)", jobName)
    FileOutputFormat.setOutputPath(job, new Path(outputDir))
    val header: Array[String] = Array("Co-Author Bin", "Number Of Co-Authors")
    System.exit(if (job.waitForCompletion(true)) 0
    else 1)
  }


  class Map extends Mapper[LongWritable, Text, Text, IntWritable] {
    private val xmlParser = SAXParserFactory.newInstance().newSAXParser()
    //private val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI

    private val one = new IntWritable(1)
    private val authorKey = new Text()

    def createValidXML(publicationString: String): String = {
      val xmlString =
        s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "dblp.dtd"><dblp>${publicationString}</dblp>"""
      println(s"xmlString = $xmlString")
      // XML.withSAXParser(xmlParser).loadString(xmlString)
      xmlString
    }


    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      logger.debug(s"Starting the map phase for $key -> ${value.toString}")

      println(value.toString)
      val publicationElement = createValidXML(value.toString)
      authorKey.set(value)
      context.write(value, one)
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
