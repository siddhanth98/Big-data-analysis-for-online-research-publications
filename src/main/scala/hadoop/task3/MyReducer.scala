package hadoop.task3

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

/**
 * This reducer will get the venue(key) and list of publication titles associated with that venue key,
 * extract the publication titles from the iterable as a single string and write the output as venue(key) and
 * publication_title_string(value)
 */
class MyReducer extends Reducer[Text, Text, Text, Text] {
  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val titles = getPublicationTitleFromValue(values)
    context.write(new Text(s"${key.toString}\t|\t"), new Text(titles))
  }

  def getPublicationTitleFromValue(values: java.lang.Iterable[Text]): String = {
    val titles = new StringBuilder
    val iter = values.iterator()
    while(iter.hasNext) titles.append("`").append(iter.next().toString).append("` ")
    titles.toString()
  }
}
