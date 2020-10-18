package hadoop.task2

import java.io.{File, FileWriter}
import java.util.Scanner

import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import scala.collection.mutable.ListBuffer

/**
 * This singleton object uses the CSVPrinter class provided by the org.apache.commons dependency to create a spreadsheet
 * of all outputs obtained from the task2 mapreduce implementation
 */
object ComputeSpreadSheet {
  def main(args: Array[String]): Unit = {
    compute(args(0))
  }

  /**
   * This function creates the printer object and uses the list of output lines to populate the spreadsheet
   * @param path The task output path passed as cmd argument
   */
  def compute(path: String): Unit = {
    val csv = new FileWriter("src/main/resources/spreadsheets/task2.csv")
    val printer = new CSVPrinter(csv, CSVFormat.DEFAULT.withHeader("Authors with 10+ consecutive years of publications"))
    val recordList = getAuthorList(path)
    recordList.foreach(record => {
      printer.print(record)
      printer.println()
    })
    csv.close()
  }

  /**
   * This reads in the job output file, and puts every line in a list and returns it
   * Note that a mutable list buffer is used to avoid running into stack overflow issues
   * if a recursive approach for an immutable list is used for a large number of lines in the output file
   * @param path The task output path passed as cmd argument
   */
  def getAuthorList(path: String): List[String] = {
    val jobOutputFile = new File(s"src/main/resources/outputs/job_outputs/task2/$path/part-r-00000")
    val scanner = new Scanner(jobOutputFile)
    val recordList: ListBuffer[String] = new ListBuffer[String]

    while(scanner.hasNext) {
      val line = scanner.nextLine().split("\t")(1).stripPrefix("`").stripSuffix("`")
      if (line.length > 0) recordList.append(line)
    }
    recordList.toList
  }
}
