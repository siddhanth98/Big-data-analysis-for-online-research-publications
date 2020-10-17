package hadoop.task4

import java.io.File

import hadoop.Constants.{localInputPathName, numInputs}
import hadoop.task4.Task4Constants._
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, KeyValueTextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.util.{Tool, ToolRunner}

object Driver extends Configured with Tool {
  def main(args: Array[String]): Unit = {
    val exitCode = ToolRunner.run(Driver, args)
    System.exit(exitCode)
  }

  @throws[Exception]
  def run(args: Array[String]): Int = {

    val job: Job = new Job()
    job.setJarByClass(Driver.getClass)
    job.setJobName("MaximumAuthorCountComputer")

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
    FileOutputFormat.setOutputPath(job, hdfsOutputPathJob)

    job.setMapperClass(classOf[MyMapper])
    job.setReducerClass(classOf[MyReducer])

    if (fs.exists(hdfsOutputPathJob)) fs.delete(hdfsOutputPathJob, true)

    val returnValue = if (job.waitForCompletion(true)) 0 else 1
    if (job.isSuccessful) println("Job successful")
    else println("Job not successful")
    if (!localOutputDir.exists())
      localOutputDir.mkdir()
    fs.copyToLocalFile(hdfsOutputPathJob, localOutputPath)
    fs.delete(hdfsOutputPathJob, true)
    returnValue
  }

  /**
   * This function will put all 138 hdfs input path dir names into a list and return it
   */
  def getConcatenatedHdfsInputPathNames(index: Int): List[String] =
    if (index == numInputs-1) List(s"input$index")
    else List(s"input$index") ::: getConcatenatedHdfsInputPathNames(index+1)

  /**
   * This function will put all 138 local input path dir relative names into a list and return it
   */
  def getConcatenatedLocalInputPathNames(index: Int): List[String] = {
    if (index == numInputs-1) List(s"$localInputPathName/input$index.txt")
    else List(s"$localInputPathName/input$index.txt") ::: getConcatenatedLocalInputPathNames(index+1)
  }
}
