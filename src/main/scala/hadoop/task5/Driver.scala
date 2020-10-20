package hadoop.task5

import java.io.File

import hadoop.Constants.{hdfsOutputPath, localInputPathName, numInputs}
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.util.{Tool, ToolRunner}
import hadoop.task5.Task5Constants._
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, KeyValueTextInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import hadoop.task6.DescendingIntWritableComparable

/**
 * This driver object defines 2 jobs -
 * The 1st job will find the total coAuthorCount of each author and write that as its output
 * The 2nd job will use a custom descendingIntWritable comparator to sort authors
 * in the descending order by their coAuthorCount output from the 1st job and will write the author and coAuthorCount
 * as the output in that order
 */
object Driver extends Configured with Tool {

  def main(args: Array[String]): Unit = {
    val exitCode = ToolRunner.run(Driver, args)
    System.exit(exitCode)
  }

  @throws[Exception]
  def run(args: Array[String]): Int = {

    val job: Job = new Job()
    job.setJarByClass(Driver.getClass)
    job.setJobName("CoAuthorCountComputer")

    val fsJob1: FileSystem = FileSystem.get(job.getConfiguration)

    val localInputPathNames = getConcatenatedLocalInputPathNames(0)
    val hdfsInputPathNames = getConcatenatedHdfsInputPathNames(0)

    val localOutputPath = new Path(localOutputPathName)
    val localOutputDir = new File(localOutputPathName)

    job.setInputFormatClass(classOf[KeyValueTextInputFormat])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.addInputPaths(job, hdfsInputPathNames.mkString(","))
    for (i <- 0 until numInputs) fsJob1.copyFromLocalFile(new Path(localInputPathNames(i)), new Path(hdfsInputPathNames(i)))
    FileOutputFormat.setOutputPath(job, hdfsOutputPath)

    job.setMapperClass(classOf[Job1Mapper])
    job.setReducerClass(classOf[Job1Reducer])

    val job2: Job = new Job()

    job2.setJarByClass(Driver.getClass)
    job2.setJobName("MaximumCoAuthorCountSort")

    job2.setInputFormatClass(classOf[TextInputFormat])
    job2.setMapOutputKeyClass(classOf[IntWritable])
    job2.setMapOutputValueClass(classOf[Text])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[IntWritable])
    job2.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.addInputPath(job2, hdfsOutputPath)
    FileOutputFormat.setOutputPath(job2, hdfsFinalOutputPath)

    job2.setMapperClass(classOf[Job2Mapper])
    job2.setSortComparatorClass(classOf[DescendingIntWritableComparable])
    job2.setReducerClass(classOf[Job2Reducer])

    val fsJob2 = FileSystem.get(job2.getConfiguration)

    if (fsJob1.exists(hdfsOutputPath)) fsJob1.delete(hdfsOutputPath, true)
    if (fsJob2.exists(hdfsFinalOutputPath)) fsJob2.delete(hdfsFinalOutputPath, true)

    val returnValue = if (job.waitForCompletion(true)) {
      fsJob1.copyToLocalFile(hdfsOutputPath, localOutputPath)
      if (job2.waitForCompletion(true)) 0
      else 1
    } else 1

    if (!localOutputDir.exists())
      localOutputDir.mkdir()

    fsJob2.copyToLocalFile(hdfsFinalOutputPath, localOutputPath)
    fsJob1.delete(hdfsOutputPath, true)
    fsJob2.delete(hdfsFinalOutputPath, true)
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
