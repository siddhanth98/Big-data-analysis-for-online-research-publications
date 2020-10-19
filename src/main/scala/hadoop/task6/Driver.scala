package hadoop.task6

import java.io.File

import hadoop.Constants.{hdfsOutputPath, localInputPathName, numInputs}
import hadoop.task6.Task6Constants.{hdfsFinalOutputPath, localOutputPathName}
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, KeyValueTextInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob
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
    job.setJobName("SingleAuthorPublicationCount")

    val fsJob1: FileSystem = FileSystem.get(job.getConfiguration)

    val localInputPathNames = getConcatenatedLocalInputPathNames(0)
    val hdfsInputPathNames = getConcatenatedHdfsInputPathNames(0)

    val localOutputPath = new Path(localOutputPathName)
    val localOutputDir = new File(localOutputPathName)

    job.setInputFormatClass(classOf[KeyValueTextInputFormat])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.addInputPaths(job, hdfsInputPathNames.mkString(","))
    for (i <- 0 until numInputs) fsJob1.copyFromLocalFile(new Path(localInputPathNames(i)), new Path(hdfsInputPathNames(i)))
    FileOutputFormat.setOutputPath(job, hdfsOutputPath)

    job.setMapperClass(classOf[MyMapper])
    job.setReducerClass(classOf[MyReducer])

    val job2: Job = new Job()
    job2.setJarByClass(Driver.getClass)
    job2.setJobName("TopAuthorsComputer")

    val fsJob2 = FileSystem.get(job2.getConfiguration)

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

    if (fsJob1.exists(hdfsOutputPath)) fsJob1.delete(hdfsOutputPath, true)
    if (fsJob2.exists(hdfsFinalOutputPath)) fsJob2.delete(hdfsFinalOutputPath, true)

    val job1Controller: ControlledJob = new ControlledJob(job.getConfiguration)
    job1Controller.setJob(job)
    val job2Controller: ControlledJob = new ControlledJob(job2.getConfiguration)
    job2Controller.setJob(job2)
    job2Controller.addDependingJob(job1Controller)

    val returnValue = if (job.waitForCompletion(true)) {
      if (job.isSuccessful && job2.waitForCompletion(true)) {
        println("Both jobs successful")
        0
      }
      else 1
    }
    else 1

    if (!localOutputDir.exists())
      localOutputDir.mkdir()

    fsJob1.delete(hdfsOutputPath, true)
    fsJob2.copyToLocalFile(hdfsFinalOutputPath, localOutputPath)
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
