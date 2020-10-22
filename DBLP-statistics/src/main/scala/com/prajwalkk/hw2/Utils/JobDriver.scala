package com.prajwalkk.hw2.Utils

import java.io.File

import com.prajwalkk.hw2.MapReduceJobs.{AuthorVenueRank, CoAuthorRanking, ContinuousAuthor, VenueHighestAuthors, VenueSingleAuthor}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

/*
*
* Created by: prajw
* Date: 17-Oct-20
*
*/
object JobDriver extends LazyLogging {
  def main(args: Array[String]): Unit = {

    if (args.size > 2) {
      val input = args(0)
      val output = args(1)
      val s = args.toList.drop(2).distinct
      logger.info(s.toString())
      val configTypesafe: Config = ConfigUtils.getConfigFile("mapredjobs.conf")
      s.foreach {

        case jobString@"Job1" => {
          logger.info("Running Venue Ranking")
          val conf = ConfigUtils.getJobTags(configTypesafe, jobString)
          AuthorVenueRank.runJob(input, output, conf)
        }

        case jobString@"Job2" => {
          logger.info("Running longest Year range above 10")
          val conf = ConfigUtils.getJobTags(configTypesafe, jobString)
          ContinuousAuthor.runJob(input, output, conf)
        }

        case jobString@"Job3" => {
          logger.info("Running Venues with Single Author publications")
          val conf = ConfigUtils.getJobTags(configTypesafe, jobString)
          VenueSingleAuthor.runJob(input, output, conf)
        }

        case jobString@"Job4" => {
          logger.info("Running Publication for each venue with highest authors:")
          val conf = ConfigUtils.getJobTags(configTypesafe, jobString)
          VenueHighestAuthors.runJob(input, output, conf)
        }

        case jobString@"Job5" => {
          logger.info("Running Co Author Ranking")
          val conf = ConfigUtils.getJobTags(configTypesafe, jobString)
          CoAuthorRanking.runJob(input, output, conf)
        }

        case jobString => logger.error(s"Invalid jobName $jobString")
      }
      System.exit(0)
    } else {
      logger.error("Usage: hadoop jar configpath Job1 Job2 ...")
      System.exit(1)
    }


  }

}
