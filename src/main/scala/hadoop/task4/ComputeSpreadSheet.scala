package hadoop.task4

import java.io.{File, FileWriter}
import java.util.Scanner

import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import scala.collection.mutable.ListBuffer

object ComputeSpreadSheet {
  def main(args: Array[String]): Unit = {
    compute(args(0))
  }

  def compute(path: String): Unit = {
    val csv = new FileWriter("src/main/resources/spreadsheets/task4.csv")
    val printer: CSVPrinter = new CSVPrinter(csv, CSVFormat.DEFAULT)
    val recordList = getVenueTitlesList(path)
    recordList.foreach(record => {
      record.foreach(e => printer.print(e))
      printer.println()
    })
    csv.close()
  }

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
