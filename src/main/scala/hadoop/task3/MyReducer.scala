package hadoop.task3

import ch.qos.logback.classic.util.ContextInitializer
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

/**
 * This reducer will get the venue(key) and list of publication titles associated with that venue key,
 * extract the publication titles from the iterable as a single string and write the output as venue(key) and
 * publication_title_string(value)
 */
class MyReducer extends Reducer[Text, Text, Text, Text] {
  System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, "src/main/resources/configuration/logback.xml")
  val logger: Logger = LoggerFactory.getLogger(classOf[MyReducer])

  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val titles = getPublicationTitleFromValue(values)
    logger.info(s"Venue - ${key.toString} Titles of single authored publications - $titles\n")
    context.write(new Text(s"${key.toString}\t|\t"), new Text(titles))
  }

  /**
   * This will stringify all aggregated publication titles for the venue and return the string
   * @param values The aggregated title names
   */
  def getPublicationTitleFromValue(values: java.lang.Iterable[Text]): String = {
    val titles = new StringBuilder
    val iter = values.iterator()
    while(iter.hasNext) titles.append("`").append(iter.next().toString).append("` ")
    titles.toString()
  }
}
