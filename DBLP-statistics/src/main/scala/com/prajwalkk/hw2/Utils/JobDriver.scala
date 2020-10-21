package com.prajwalkk.hw2.Utils

import com.prajwalkk.hw2.MapReduceJobs.{AuthorVenueRank, CoAuthorRanking, ContinuousAuthor, VenueHighestAuthors, VenueSingleAuthor}
import com.typesafe.scalalogging.LazyLogging

/*
*
* Created by: prajw
* Date: 17-Oct-20
*
*/
object JobDriver extends LazyLogging {
  def main(args: Array[String]): Unit = {

    if (args.size > 0) {
      val s = args.toList.distinct
      s.foreach { jobString =>


        jobString match {


          case "Job1" => {
            ConfigUtils.getJobTags("Job1")
            logger.info("Running Venue Ranking")
            val conf = ConfigUtils.getJobTags(jobString)
            AuthorVenueRank.runJob(conf)
          }

          case "Job2" => {
            ConfigUtils.getJobTags("Job2")
            logger.info("Running longest Year range above 10")
            val conf = ConfigUtils.getJobTags(jobString)
            ContinuousAuthor.runJob(conf)
          }

          case "Job3" => {
            ConfigUtils.getJobTags("Job3")
            logger.info("Running Venues with Single Author publications")
            val conf = ConfigUtils.getJobTags(jobString)
            VenueSingleAuthor.runJob(conf)
          }

          case "Job4" => {
            ConfigUtils.getJobTags("Job4")
            logger.info("Running Puglication for each venue with highest authors:")
            val conf = ConfigUtils.getJobTags(jobString)
            VenueHighestAuthors.runJob(conf)
          }

          case "Job5" => {
            ConfigUtils.getJobTags("Job5")
            logger.info("Running Co Author Ranking")
            val conf = ConfigUtils.getJobTags(jobString)
            CoAuthorRanking.runJob(conf)
          }

          case _ => logger.error(s"Invalid jobName $jobString")
        }
      }

    } else {
      logger.error("hadoop jar Job<n>")
    }


  }

}
