package com.prajwalkk.hw2.Utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

/*
*
* Created by: prajw
* Date: 18-Oct-20
*
*/
object HdfsIoUtils extends LazyLogging{
  def writeToHDFS(output: String, csvFile: String, content: ArrayBuffer[String]) = {
    val fs = FileSystem.get(new Configuration)
    val hdfsPath = new Path(output + Path.SEPARATOR + csvFile + ".csv")
    val outputstream = fs.create(hdfsPath)
    content.foreach(row => outputstream.writeBytes(row))
    outputstream.close()
  }

  def readHdfsPartFiles(input: String): String = {
    val fs = FileSystem.get(new Configuration())
    val status: Array[FileStatus] = fs.listStatus(new Path(input))
    val files = status.filter(f => f.getPath.getName.startsWith("part-"))
    val lines = new StringBuilder
    for (k <- files.indices) {
      val stream = fs.open(files(k).getPath)
      val readLines: BufferedSource = Source.fromInputStream(stream)
      lines.append(readLines.mkString)
      lines.append("\n")
    }
    fs.close()
    lines.toString()
  }


}
