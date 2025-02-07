package hadoop.task4

import java.io.{File, FileWriter}
import java.util.Scanner

import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import scala.collection.mutable.ListBuffer

/**
 * This singleton object uses the CSVPrinter class provided by the org.apache.commons dependency to create a spreadsheet
 * of all outputs obtained from the task4 mapreduce implementation
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
    val csv = new FileWriter("src/main/resources/spreadsheets/task4.csv")
    val printer: CSVPrinter = new CSVPrinter(csv, CSVFormat.DEFAULT.withHeader("Venue", "List of publications"))
    val recordList = getVenueTitlesList(path)
    recordList.foreach(record => {
      record.foreach(e => printer.print(e))
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
  def getVenueTitlesList(path: String): List[List[String]] = {
    val recordList: ListBuffer[List[String]] = new ListBuffer[List[String]]
    val scanner = new Scanner(new File(s"src/main/resources/outputs/job_outputs/task4/$path/part-r-00000"))
    while(scanner.hasNext) {
      val record = scanner.nextLine().split("` ")
        .map(e => e.strip().stripPrefix("`").stripSuffix("`")).toList
      recordList.append(record)
    }
    recordList.toList
  }
}
