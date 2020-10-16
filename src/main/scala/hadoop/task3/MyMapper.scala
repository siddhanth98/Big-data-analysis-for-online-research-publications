package hadoop.task3

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

/**
 * This mapper will get the publication title, year and venue as parts of the key and the list of authors as parts of value
 * and if the record only has one author, then it will write the output as venue(key) publication_title(value)
 */
class MyMapper extends Mapper[Text, Text, Text, Text] {
  override def map(key: Text, value: Text, context: Mapper[Text, Text, Text, Text]#Context): Unit = {
    if (value.toString.split("` ").length == 1) {
      context.write(new Text(extractVenue(key.toString)),
        new Text(s"${extractPublicationTitle(key.toString)}"))
    }
  }
  def extractVenue(key: String): String = key.split("` ")(2).stripPrefix("`").stripSuffix("`")
  def extractPublicationTitle(key: String): String = key.split("` ")(0).stripPrefix("`").stripSuffix("`")
}
