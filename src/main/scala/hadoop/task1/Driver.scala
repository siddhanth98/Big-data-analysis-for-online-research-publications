package hadoop.task1

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, KeyValueTextInputFormat, NLineInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.apache.commons.io.FileUtils
import java.io.File

import hadoop.task1.Constants._

object Driver extends Configured with Tool {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val exitCode: Int = ToolRunner.run(Driver, args)
    System.exit(exitCode)
  }

  @throws[Exception]
  def run(args: Array[String]): Int = {

    val job: Job = new Job()
    job.setJarByClass(Driver.getClass)
    job.setJobName("Task1")

    val fs: FileSystem = FileSystem.get(job.getConfiguration)

    val localInputPathNames = getConcatenatedLocalInputPathNames(0)
    val hdfsInputPathNames = getConcatenatedHdfsInputPathNames(0)

    val localOutputPath = new Path(localOutputPathName)

    val localOutputDir = new File(localOutputPathName)

    job.setInputFormatClass(classOf[KeyValueTextInputFormat])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])

    FileInputFormat.addInputPaths(job, hdfsInputPathNames.mkString(","))
    for (i <- 0 until numInputs) fs.copyFromLocalFile(new Path(localInputPathNames(i)), new Path(hdfsInputPathNames(i)))
    FileOutputFormat.setOutputPath(job, hdfsOutputPath)

    job.setMapperClass(classOf[MyMapper])
    job.setReducerClass(classOf[MyReducer])

    val returnValue: Int = if (job.waitForCompletion(true)) 0 else 1
    val numInputSplits = NLineInputFormat.getNumLinesPerSplit(job)
    println(numInputSplits)

    if (job.isSuccessful) println("Job was successful")
    else println("Job was not successful")

    if (localOutputDir.exists()) {
      FileUtils.deleteDirectory(localOutputDir)
      localOutputDir.mkdir()
    }
    fs.copyToLocalFile(hdfsOutputPath, localOutputPath)

    fs.delete(hdfsOutputPath, true)
    returnValue
  }

  def getConcatenatedHdfsInputPathNames(index: Int): List[String] =
    if (index == numInputs-1) List(s"input$index")
    else List(s"input$index") ::: getConcatenatedHdfsInputPathNames(index+1)

  def getConcatenatedLocalInputPathNames(index: Int): List[String] = {
    if (index == numInputs-1) List(s"$localInputPathName/input$index.txt")
    else List(s"$localInputPathName/input$index.txt") ::: getConcatenatedLocalInputPathNames(index+1)
  }
}
