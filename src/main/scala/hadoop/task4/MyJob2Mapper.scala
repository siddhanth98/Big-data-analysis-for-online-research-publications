package hadoop.task4

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

import scala.collection.mutable.ListBuffer

class MyJob2Mapper extends Mapper[Text, Text, Text, Text] {
  override def map(key: Text, value: Text, context: Mapper[Text, Text, Text, Text]#Context): Unit = {
    val maxAuthorCount = getMaxAuthorCount(key)
    val maxAuthorTitles = getMaxAuthorTitles(value, maxAuthorCount)
    context.write(new Text(extractVenue(key)), new Text(maxAuthorTitles))
  }

  def extractVenue(key: Text): String = key.toString.strip().split("` ")(0).strip().stripPrefix("`")

  def getMaxAuthorCount(key: Text): Int = key.toString.strip().split("` ")(1).stripPrefix("`").stripSuffix("`").toInt

  def getMaxAuthorTitles(value: Text, maxAuthorCount: Int): String = {
    val resultList = new StringBuilder()

    value.toString.split("` ").foreach(e => {
      if (e.split("\t")(1).strip().stripSuffix("`").toInt == maxAuthorCount)
        resultList.append("`").append(e.split("\t")(0).strip().stripPrefix("`")).append("` ")
    })
    resultList.toString()
  }
}
