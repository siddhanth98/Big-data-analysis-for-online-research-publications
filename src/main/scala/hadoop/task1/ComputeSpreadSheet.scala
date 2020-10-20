package hadoop.task1

import java.io.{File, FileReader, FileWriter, IOException}
import java.util.Scanner

import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * This singleton object uses the CSVPrinter class provided by the org.apache.commons dependency to create a spreadsheet
 * of all outputs obtained from the task1 mapreduce implementation
 */
object ComputeSpreadSheet {

  def main(args: Array[String]): Unit = {
    compute(args(0))
  }

  /**
   * This function creates the printer object and uses the list of output lines to populate the spreadsheet
   * @param path The task output path passed as cmd argument
   */
  @throws[IOException]
  def compute(path: String): Unit = {
    val venueAuthorList = getVenueAndAuthors(path)
    val writer: FileWriter = new FileWriter("src/main/resources/spreadsheets/task1.csv")
    val printer: CSVPrinter =
      new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("Venue", "Author1", "Author2", "Author3", "Author4", "Author5",
      "Author6", "Author7", "Author8", "Author9", "Author10"))

    venueAuthorList.foreach(record => {
      record.foreach(e => printer.print(e))
      printer.println()
    })
    writer.close()
  }

  /**
   * This reads in the job output file, and puts every line in a list and returns it
   * Note that a mutable list buffer is used to avoid running into stack overflow issues
   * if a recursive approach for an immutable list is used for a large number of lines in the output file
   * @param path The task output path passed as cmd argument
   */
  def getVenueAndAuthors(path: String): List[List[String]] = {
    val outputFile: File = new File(s"src/main/resources/outputs/job_outputs/task1/$path/part-r-00000")
    val reader: Scanner = new Scanner(outputFile)
    val venueAuthorList: ListBuffer[List[String]] = new ListBuffer[List[String]]

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
