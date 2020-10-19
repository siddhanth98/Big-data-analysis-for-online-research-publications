package hadoop.task5

import java.io.{File, FileWriter}
import java.util.Scanner

import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import scala.annotation.tailrec

object ComputeSpreadSheet {
  def main(args: Array[String]): Unit = compute(args(0))

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
