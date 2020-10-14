package hadoop.task1

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.mutable
import scala.util.Sorting

class MyReducer extends Reducer[Text, Text, Text, Text] {

  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val title = extractPublicationTitle(key.toString)
    val year = extractPublicationYear(key.toString)
    val venue = extractPublicationVenue(key.toString)
    val iter = values.iterator()
    val authorList = getAuthorListFromInput(iter)
    authorList.foreach(author => author.stripPrefix("`").stripSuffix("`"))
    val authorCountMap = mutable.Map[String, Int]()
    authorList.foreach (author => authorCountMap += (author -> (authorCountMap.getOrElse(author, 0)+1)))
    val finalAuthorNames = getTopAuthors(authorCountMap)
    context.write(new Text(venue), new Text(finalAuthorNames))
  }

  def extractPublicationTitle(key: String): String = key.split("` ")(0).stripPrefix("`").stripSuffix("`")
  def extractPublicationYear(key: String): String = key.split("` ")(1).stripPrefix("`").stripSuffix("`")
  def extractPublicationVenue(key: String): String = key.split("` ")(2).stripPrefix("`").stripSuffix("`")

  def getAuthorListFromInput(iter: java.util.Iterator[Text]): List[String] =
    if (iter.hasNext) iter.next().toString.split(" ").toList ::: getAuthorListFromInput(iter)
    else List.empty

  def getTopAuthors(authorCountMap: mutable.Map[String, Int]): String = {
    val topAuthors = getAuthorsByNumberOfPublications(authorCountMap)
    println(s"Top authors obtained are:")
    topAuthors.foreach(e => println(e.mkString(" ")))

    val authorNames = getConcatenatedAuthorNames(topAuthors, "")
    println(s"top author names are $authorNames")
    authorNames
  }

  def getConcatenatedAuthorNames(a: Array[Array[String]], res: String): String =
    if (a.length == 1) res + a(0)(0)
    else getConcatenatedAuthorNames(a.slice(1, a.length), res + a(0)(0) + " ")

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
