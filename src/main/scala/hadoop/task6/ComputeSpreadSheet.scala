package hadoop.task6

import java.io.{File, FileWriter}
import java.util.Scanner

import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import scala.collection.mutable.ListBuffer

object ComputeSpreadSheet {
  def main(args: Array[String]): Unit = {
    compute(args(0))
  }

  def compute(path: String): Unit = {
    val csv = new FileWriter("src/main/resources/spreadsheets/task6.csv")
    val printer = new CSVPrinter(csv, CSVFormat.DEFAULT.withHeader("Authors", "Number of solo publications"))
    val recordList = getRecordList(path)
    recordList.foreach(record => {
      printer.print(record(0))
      printer.print(record(1))
      printer.println()
    })
    csv.close()
  }

  def getRecordList(path: String): List[List[String]] = {
    val jobOutputFile = new File(s"src/main/resources/outputs/job_outputs/task6/$path/part-r-00000")
    val scanner = new Scanner(jobOutputFile)
    val recordList: ListBuffer[List[String]] = ListBuffer[List[String]]()
    while(scanner.hasNext) {
      val line = scanner.nextLine().split("`\t")
      val author = line(0).strip().stripPrefix("`").stripSuffix("`")
      val count = line(1).strip().stripSuffix("`").stripSuffix("`")
      recordList.append(List(author, count))
    }
    recordList.toList
  }
}
