package hadoop.task4

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.mutable.ListBuffer

/**
 * The reducer will take in the shuffled and sorted mapper output key value pairs as :
 * `venue` (key) : `title1 3` `title2 4` `title3 5` ...
 * It will one pass over all title details in the value to find the maximum number of authors that the venue has
 * Then it will make one more pass over the titles and collect all titles whose count matches the maximum value
 * It will give the output as - `venue` (key) : `title1` `title2` `title3` `title4`...
 */
class MyReducer extends Reducer[Text, Text, Text, Text] {
  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text,Text]#Context): Unit = {
    val valuesList = getListFromIterator(values)
    val maxAuthorCountTitles = getMaxAuthorCountTitles(valuesList, getMaxAuthorCount(valuesList))
    context.write(key, new Text(maxAuthorCountTitles))
  }

  /**
   * This function will collect all titles whose author count matches the maximum count value
   * @param values The value iterator converted into a list
   * @param maxAuthorCount The maximum author count value
   */
  def getMaxAuthorCountTitles(values: List[String], maxAuthorCount: Int): String = {
    val resultList = new StringBuilder()
    values.foreach(e => {
      if (e.split("\t")(1).strip().stripSuffix("`").toInt == maxAuthorCount)
        resultList.append("`").append(e.split("\t")(0).strip().stripPrefix("`")).append("` ")
    })
    resultList.toString()
  }

  /**
   * This function will convert the iterable of values into a list and return it
   * @param values The iterable of values collected from all mappers
   */
  def getListFromIterator(values: java.lang.Iterable[Text]): List[String] = {
    val resultList: ListBuffer[String] = new ListBuffer[String]
    val iter = values.iterator()
    while(iter.hasNext) resultList.prepend(iter.next().toString)
    resultList.reverse.toList
  }

  /**
   * This function will stringify all list values into a single string
   * EDIT : This function is currently not used in the reduce function
   * @param values The list of values
   */
  def getChainedListValues(values: List[String]): String = {
    val result = new StringBuilder()
    values.foreach(e => result.append(e))
    result.toString()
  }

  /**
   *  This function will make the 1st pass over all values to find the maximum number of authors that a title(s) has
   *  for the venue
   */
  def getMaxAuthorCount(values: List[String]): Int = {
    val authorCountList: ListBuffer[Int] = new ListBuffer[Int]
    values.foreach(e => authorCountList.append(e.split("\t")(1).strip().stripSuffix("`").toInt))
    authorCountList.max
  }
}
