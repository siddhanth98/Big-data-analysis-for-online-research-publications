package hadoop.task5

import java.io.{File, FileWriter}
import java.util.Scanner

import org.apache.commons.csv.{CSVFormat, CSVPrinter}

/**
 * This singleton object uses the CSVPrinter class provided by the org.apache.commons dependency to create a spreadsheet
 * of all outputs obtained from the task5 mapreduce implementation
 */
object ComputeSpreadSheet {
  def main(args: Array[String]): Unit = compute(args(0))

  /**
   * This function creates the printer object and uses the list of output lines to populate the spreadsheet
   * @param path The task output path passed as cmd argument
   */
  def compute(path: String): Unit = {
    val csv = new FileWriter("src/main/resources/spreadsheets/task5.csv")
    val printer = new CSVPrinter(csv, CSVFormat.DEFAULT.withHeader("Authors", "Total number of co authors"))
    val recordList = getRecordList(path)
    recordList.foreach(record => {
      printer.print(record(0))
      printer.print(record(1))
      printer.println()
    })
    csv.close()
  }

  def getRecordList(path: String): List[List[String]] = {
    val jobOutputFile = new File(s"src/main/resources/outputs/job_outputs/task5/$path/part-r-00000")
    val scanner = new Scanner(jobOutputFile)
    getRecordList(scanner, 0)
  }

  /**
   * This reads in the job output file, and puts every line in a list and returns it
   * Note that a mutable list buffer is used to avoid running into stack overflow issues
   * if a recursive approach for an immutable list is used for a large number of lines in the output file
   * @param sc The input file reader
   * @param count The accumulator variable representing the number of lines read so far
   */

  def getRecordList(sc: Scanner, count: Int): List[List[String]] = {
    if (sc.hasNext && count < 100) {
      val line = sc.nextLine.split("`\t")
      val author = line(0).strip().stripPrefix("`").stripSuffix("`") + " "
      val coAuthorCount = line(1).strip().toInt
      println(author + " " + coAuthorCount)
      (List(author, coAuthorCount.toString) :: getRecordList(sc, count+1))
    }
    else List.empty
  }
}
