package hadoop.task1

import java.io.{File, FileReader, FileWriter, IOException}
import java.util.Scanner

import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ComputeSpreadSheet {

  def main(args: Array[String]): Unit = {
    compute(args(0))
  }

  @throws[IOException]
  def compute(path: String): Unit = {
    val venueAuthorList = getVenueAndAuthors(path)
    val writer: FileWriter = new FileWriter("src/main/resources/spreadsheets/task1.csv")
    val printer: CSVPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT)
    venueAuthorList.foreach(record => {
      record.foreach(e => printer.print(e))
      printer.println()
    })
    writer.close()
  }

  def getVenueAndAuthors(path: String): List[List[String]] = {
    val outputFile: File = new File(s"src/main/resources/outputs/job_outputs/task1/$path/part-r-00000")
    val reader: Scanner = new Scanner(outputFile)
    val venueAuthorList: mutable.ListBuffer[List[String]] = new ListBuffer[List[String]]

    while(reader.hasNextLine) {
      val line = reader.nextLine().split("\\|")
      val venue = line(0).strip()
      val authorList = line(1).strip().split("` ")
        .map(author => author.stripPrefix("`").stripSuffix("`"))
        .filter(author => author.length > 0)
        .toList
      val recordList = venue :: authorList
      venueAuthorList.prepend(recordList)
    }
    venueAuthorList.toList.reverse
  }
}
