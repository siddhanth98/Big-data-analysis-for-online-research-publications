package hadoop.task1

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class MyMapper extends Mapper[Text, Text, Text, Text] {
  override def map(key: Text, value: Text, context: Mapper[Text, Text, Text, Text]#Context): Unit = {
    val venue = extractPublicationVenue(key.toString)
    context.write(new Text(venue), value)
  }
  def extractPublicationVenue(key: String): String = key.split("` ")(2).stripPrefix("`").stripSuffix("`")
}
