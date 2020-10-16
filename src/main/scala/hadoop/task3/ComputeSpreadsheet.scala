package hadoop.task3

import java.io.{File, FileWriter}
import java.util.Scanner

import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import scala.collection.mutable

object ComputeSpreadsheet {

  def main(args: Array[String]): Unit = {
    compute()
  }

  def compute(): Unit = {
    val csv = new FileWriter("src/main/resources/spreadsheets/task3.csv")
    val printer: CSVPrinter = new CSVPrinter(csv, CSVFormat.DEFAULT)
    val recordList = getVenueAndPublicationsList
    recordList.foreach(record => {
      record.foreach(e => printer.print(e))
      printer.println()
    })
    csv.close()
  }

  def getVenueAndPublicationsList: List[List[String]] = {
    val hadoopOutputFile = new File("src/main/resources/outputs/job_outputs/task3/output/part-r-00000")
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
