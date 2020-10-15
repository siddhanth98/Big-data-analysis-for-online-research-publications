package hadoop.task1

import ch.qos.logback.classic.util.ContextInitializer
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.util.Sorting

/**
 * This reducer will receive Venue -> List of author names (possibly with repeated author names as values of same keys will be combined)
 * It uses a mutable map to track author counts for all unique authors and sorts in descending order and writes the
 * venue as its output key and sorted author names list as its output value
 */
class MyReducer extends Reducer[Text, Text, Text, Text] {
  /*System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, "src/main/resources/configuration/logback.xml")
  val logger: Logger = LoggerFactory.getLogger(classOf[MyReducer])*/

  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {

    val iter = values.iterator()
    val authorList = getAuthorListFromInput(iter)
    val authorCountMap = mutable.Map[String, Int]()
    authorList.foreach (author => authorCountMap += (author -> (authorCountMap.getOrElse(author, 0)+1)))
    val finalAuthorNames = getTopAuthors(authorCountMap)
//    logger.info(s"Venue - $key -> Authors - $finalAuthorNames")
    context.write(new Text(s"$key\t|\t"), new Text(finalAuthorNames))
  }

  /**
   * Extract name of publication from key
   */
  def extractPublicationTitle(key: String): String = key.split("` ")(0).stripPrefix("`").stripSuffix("`")
  /**
   * Extract year of publication from key
   */
  def extractPublicationYear(key: String): String = key.split("` ")(1).stripPrefix("`").stripSuffix("`")
  /**
   * Extract venue of publication from key
   */
  def extractPublicationVenue(key: String): String = key.split("` ")(2).stripPrefix("`").stripSuffix("`")

  /**
   * Value input to the reducer will be as - Iterable[author1 author2, author2 author3 author 4, author5 author6]
   * This function will serialize it into a single list as - List[author1, author2, author2, author3, author4, author5, author6]
   * @param iter The iterator
   */
  def getAuthorListFromInput(iter: java.util.Iterator[Text]): List[String] = {
    val resultList: mutable.ListBuffer[String] = new mutable.ListBuffer[String]
    while(iter.hasNext) iter.next().toString.split("` ").toList.foreach(author => resultList.prepend("`".concat(author.stripPrefix("`").stripSuffix("`")).concat("`")))
    resultList.toList.reverse
  }

  /**
   * This function will sort the authors from the map and combines them into 1 single string and return the string
   * @param authorCountMap The map of author names to number of occurrences
   */
  def getTopAuthors(authorCountMap: mutable.Map[String, Int]): String = {
    val topAuthors = getAuthorsByNumberOfPublications(authorCountMap)
    topAuthors.foreach(e => println(e.mkString(" ")))

    val authorNames = getConcatenatedAuthorNames(topAuthors, "")
    authorNames
  }

  /**
   * Recursive function to concatenate author names from a 2d array - [[auth1, 1], [[auth2, 2]] -> auth1 auth2
   */
  def getConcatenatedAuthorNames(a: Array[Array[String]], res: String): String =
    if (a.length == 1) res + a(0)(0)
    else getConcatenatedAuthorNames(a.slice(1, a.length), res + a(0)(0) + " ")

  /**
   * Put the authors from the map into a 2d array to be used for sorting
   * @param authorCountMap Map of author names to count
   */
  def getAuthorsByNumberOfPublications(authorCountMap: mutable.Map[String, Int]): Array[Array[String]] = {
    object CountOrdering extends Ordering[Array[String]] {
      def compare(a1: Array[String], a2: Array[String]): Int = a2(1).toInt compareTo a1(1).toInt
    }

    val authorCountArray = new Array[Array[String]](authorCountMap.keySet.size)
    var index = 0

    authorCountMap.foreach { e =>
      authorCountArray(index) = Array(e._1, e._2.toString)
      index += 1
    }
    Sorting.quickSort(authorCountArray)(CountOrdering)
    authorCountArray.slice(0, 10)
  }
}
