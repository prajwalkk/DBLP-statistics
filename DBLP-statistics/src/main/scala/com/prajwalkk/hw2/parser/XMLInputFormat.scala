package com.prajwalkk.hw2.parser

import java.nio.charset.StandardCharsets

import com.google.common.io.Closeables
import com.prajwalkk.hw2.parser.XMLInputFormat.XMLRecordReader
import com.typesafe.scalalogging.{LazyLogging}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.{DataOutputBuffer, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}


class XMLInputFormat extends TextInputFormat with LazyLogging {


  override def createRecordReader(split: InputSplit,
                                  context: TaskAttemptContext): RecordReader[LongWritable, Text] =
    new XMLRecordReader(split.asInstanceOf[FileSplit], context.getConfiguration)

}
object XMLInputFormat extends LazyLogging {

  val START_TAG_KEY: String = "xmlinput.start"

  val END_TAG_KEY: String = "xmlinput.end"


  /**
   * XMLRecordReader class to read through a given xml document to output xml
   * blocks as records as specified by the start tag and end tag
   *
   * Credits @link{github.com/mayankrastogi/faculty-collaboration/}
   */
  class XMLRecordReader(split: FileSplit, jobConf: Configuration)
    extends RecordReader[LongWritable, Text] {


    // Start and end tags of the XML from the job driver in mapreduce
    private val startTag = jobConf.getStrings(START_TAG_KEY).map(_.getBytes(StandardCharsets.UTF_8))
    private val endTag = jobConf.getStrings(END_TAG_KEY).map(_.getBytes(StandardCharsets.UTF_8))
    private val startTagToEndTagMapping = startTag.zip(endTag).toMap


    // File pointers pointing to start and end of file
    // File handling tasks
    private val file: Path = split.getPath
    private val fs: FileSystem = file.getFileSystem(jobConf)
    private val start: Long = split.getStart
    private val end: Long = start + split.getLength
    private val fsin: FSDataInputStream = fs.open(split.getPath)

    // Buffer stores intermediate shard content
    private val buffer: DataOutputBuffer = new DataOutputBuffer()

    private val currentKey: LongWritable = new LongWritable()
    private val currentValue: Text = new Text()
    private var matchedTag = Array[Byte]()


    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
      fsin.seek(start)
    }

    override def nextKeyValue(): Boolean = {
      next(currentKey, currentValue)
    }

    private def next(key: LongWritable, value: Text): Boolean = {
      if (fsin.getPos < end && readUntilMatch(startTag, false)) {
        try {
          buffer.write(matchedTag)
          if (readUntilMatch(Array(startTagToEndTagMapping(matchedTag)), true)) {
            key.set(fsin.getPos)
            value.set(buffer.getData, 0, buffer.getLength)
            return true
          }
        } finally buffer.reset()
      }
      false
    }

    private def readUntilMatch(tags: Array[Array[Byte]], lookingForEndTag: Boolean): Boolean = {
      val matchCounter: Array[Int] = Array.fill(tags.length)(0)

      while (true) {
        // Read a byte from the input stream
        val currentByte = fsin.read()
        // Return false if end of file is reached
        if (currentByte == -1) {
          return false
        }
        // If we are looking for the end tag, buffer the file contents until we find it.
        if (lookingForEndTag) {
          buffer.write(currentByte)
        }
        // Check if we are matching any of the tags
        tags.indices.foreach { tagIndex =>
          // The current tag which we are testing for a match
          val tag = tags(tagIndex)
          if (currentByte == tag(matchCounter(tagIndex))) {
            matchCounter(tagIndex) += 1
            // If the counter for this tag reaches the length of the tag, we have found a match
            if (matchCounter(tagIndex) >= tag.length) {
              matchedTag = tag
              return true
            }
          }
          else {
            // Reset the counter for this tag if the current byte doesn't match with the byte of the current tag being
            // tested
            matchCounter(tagIndex) = 0
          }
        }
        // Check if we've passed the stop point
        if (!lookingForEndTag && matchCounter.forall(_ == 0) && fsin.getPos >= end) {
          return false
        }
      }
      false
    }

    override def getCurrentKey: LongWritable = {
      new LongWritable(currentKey.get())
    }

    override def getCurrentValue: Text = {
      new Text(currentValue)
    }

    override def getProgress: Float = {
      (fsin.getPos - start) / (end - start).toFloat
    }

    override def close(): Unit = {
      Closeables.close(fsin, true)
    }
  }
}

/**
 * Reads records that are delimited by a specifc begin/end tag.
 */


