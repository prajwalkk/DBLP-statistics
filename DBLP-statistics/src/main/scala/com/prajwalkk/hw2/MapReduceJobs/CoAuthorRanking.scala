package com.prajwalkk.hw2.MapReduceJobs

import java.lang

import com.prajwalkk.hw2.Utils.{ConfigUtils, Constants}
import com.prajwalkk.hw2.Utils.XMLUtils.{createValidXML, extractAuthors}
import com.prajwalkk.hw2.parser.XMLInputFormat
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, IntWritable, LongWritable, Text}
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
object CoAuthorRanking extends LazyLogging {

  logger.debug(s"${this.getClass}: Mapper phase Initiated")

  /**
   * Ranks authors based off co-author count.
   * Output for this would be author -> num of unique co authors
   * Then calls a sorting job to sort it in descending order based on the count
   *
   * @param configTypesafe
   */
  def runJob(configTypesafe: Config): Unit = {
    val input: String = configTypesafe.getString(Constants.INPUT_PATH)
    val output: String = configTypesafe.getString(Constants.OUTPUT_PATH)
    val outputSeperator = configTypesafe.getString(Constants.SEPERATOR)

    logger.debug(s"${this.getClass}: Job started")
    val conf = new Configuration
    conf.set("mapreduce.output.textoutputformat.separator", configTypesafe.getString(outputSeperator))
    conf.set("mapreduce.map.log.level", "WARN")
    conf.set("mapreduce.reduce.log.level", "WARN")

    conf.setStrings(XMLInputFormat.START_TAG_KEY, ConfigUtils.getXMLtags(Constants.XML_START_TAG): _*)
    conf.setStrings(XMLInputFormat.END_TAG_KEY, ConfigUtils.getXMLtags(Constants.XML_END_TAG): _*)
    val jobName = configTypesafe.getString(Constants.NAME)
    val job = Job.getInstance(conf, jobName)

    job.setJarByClass(this.getClass)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])

    job.setMapperClass(classOf[this.AuthScoreMapper])
    job.setReducerClass(classOf[this.AuthScoreReducer])

    job.setInputFormatClass(classOf[XMLInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])

    FileInputFormat.addInputPath(job, new Path(input))
    val outputDir = output + jobName + Path.SEPARATOR
    FileOutputFormat.setOutputPath(job, new Path(outputDir))
    val header = Array("Top 100 Authors who publish with most Co-authors",
      "Top 100 Authors who publish with least Co-authors")
    if (job.waitForCompletion(true)) {
      logger.info("Running Sorting Job")
      SortingJob.runJob(outputDir, output, jobName)
      //HdfsIoUtils.writeToHDFS(outputDir, jobName, ParserUtils.formatCsv(header, getFinalCSV(output.replace("(jobName)", "Sorted" + jobName))))
    } else System.exit(1)
  }


  class AuthScoreMapper extends Mapper[LongWritable, Text, Text, Text] {
    logger.debug(s"${this.getClass}: Mapper phase initiated")

    override def map(key: LongWritable,
                     value: Text,
                     context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      logger.debug(s"${this.getClass}: Mapper phase started")
      val publicationElement = createValidXML(value.toString)
      val authors = extractAuthors(publicationElement).distinct.toList

      // generate all permutations of 2 coauthors. The scala permutation function is nPn only.
      // But there exists nCr and r can be changed.
      // nPr =  fact(r) * nCr => nP2 = 2 *  nC2. Therefore generate all permutations for each combination of author pair.
      // You will get permutations of 2 authors. This is possible with 2. else we have to take factorial of any other number
      if (authors.size > 1) {
        val authCombinations = authors.combinations(2).toList
        authCombinations.foreach { combi =>
          combi.permutations.foreach { perm =>
            logger.info(s"Mapper emit: ${perm(0)} -> ${perm(0)}")
            context.write(new Text(perm(0)), new Text(perm(1)))
          }
        }
      } else if (authors.size == 1) {
        // Populate with a trash value which can be removed later. for a single author publication
        logger.info(s"Mapper emit: $authors -> single")
        context.write(new Text(authors(0)), new Text("singlecontributionforthis"))
      }
    }
  }

  class AuthScoreReducer extends Reducer[Text, Text, Text, IntWritable] {
    logger.debug(s"${this.getClass}: Reducer phase initiated")

    override def reduce(key: Text,
                        values: lang.Iterable[Text],
                        context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {
      logger.debug(s"${this.getClass}: Reducer phase started")
      logger.info(s"Reducer Input: $key -> ${values.toString}")
      val coAuthors = values.asScala.map(value => value.toString).toList.distinct
      // get the coauthor count for this author
      val numCoAuth = coAuthors.filter(_ != "singlecontributionforthis").size
      // remove the -1 multiplier
      context.write(key, new IntWritable(numCoAuth))
      logger.info(s"Reducer emit: ${key.toString} -> $numCoAuth")
    }
  }


}
