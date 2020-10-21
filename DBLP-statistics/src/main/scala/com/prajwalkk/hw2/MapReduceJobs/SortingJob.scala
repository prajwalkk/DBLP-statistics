package com.prajwalkk.hw2.MapReduceJobs

import java.lang

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

/*
*
* Created by: prajw
* Date: 18-Oct-20
*
*/
object SortingJob extends LazyLogging {

  logger.debug(s"${this.getClass}: Job Initated")

  def runJob(input: String, output: String, inputJobName: String): Unit = {
    val conf = new Configuration()
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    conf.set("mapreduce.map.log.level", "WARN")
    conf.set("mapreduce.reduce.log.level", "WARN")
    val jobName = "Sorted" + inputJobName
    val job = Job.getInstance(conf, jobName)

    job.setJarByClass(SortingJob.getClass)

    job.setOutputKeyClass(classOf[IntWritable])
    job.setOutputValueClass(classOf[Text])

    job.setMapOutputKeyClass(classOf[IntWritable])
    job.setMapOutputValueClass(classOf[Text])

    job.setMapperClass(classOf[SortingJob.SortMap])
    job.setReducerClass(classOf[SortingJob.SortReduce])


    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])

    FileInputFormat.addInputPath(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(output + jobName + "/"))
    if (job.waitForCompletion(true)) return else System.exit(1)
  }

  class SortMap extends Mapper[LongWritable, Text, IntWritable, Text] {
    logger.debug(s"${this.getClass}: Mapper phase initiated")

    override def map(key: LongWritable,
                     value: Text,
                     context: Mapper[LongWritable, Text, IntWritable, Text]#Context): Unit = {
      logger.debug(s"${this.getClass}: Mapper phase started")
      val line = value.toString
      try {
        val Array(author, score) = line.split(",")
        // reverse sorting hence -1
        context.write(new IntWritable(score.toInt * -1), new Text(author))
      } catch {
        case me: MatchError =>
      }
    }
  }

  class SortReduce extends Reducer[IntWritable, Text, Text, IntWritable] {
    logger.debug(s"${this.getClass}: Mapper phase initiated")

    override def reduce(key: IntWritable,
                        values: lang.Iterable[Text],
                        context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
      logger.debug(s"${this.getClass}: Reducer phase started")
      values.forEach(value => context.write(value, key))
    }
  }


}
