package hadoop.task3

import java.io.{File, FileWriter}
import java.util.Scanner

import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import scala.collection.mutable

/**
 * This singleton object uses the CSVPrinter class provided by the org.apache.commons dependency to create a spreadsheet
 * of all outputs obtained from the task3 mapreduce implementation
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
    val csv = new FileWriter("src/main/resources/spreadsheets/task3.csv")
    val printer: CSVPrinter = new CSVPrinter(csv, CSVFormat.DEFAULT.withHeader("Venue", "List of publications"))
    val recordList = getVenueAndPublicationsList(path)
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
  def getVenueAndPublicationsList(path: String): List[List[String]] = {
    val hadoopOutputFile = new File(s"src/main/resources/outputs/job_outputs/task3/$path/part-r-00000")
    val scanner = new Scanner(hadoopOutputFile)
    val recordList = new mutable.ListBuffer[List[String]]

    while(scanner.hasNext) {
      val recordOutput = scanner.nextLine().split("\\|")
      val venue = recordOutput(0).strip()
      val publicationList = recordOutput(1).strip().split("` ").map(e => e.stripPrefix("`").stripSuffix("`")).toList
      val record = List(venue) ::: publicationList
      recordList.prepend(record)
    }
    recordList.toList
  }
}
