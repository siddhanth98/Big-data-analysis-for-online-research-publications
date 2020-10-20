package hadoop.task3

import ch.qos.logback.classic.util.ContextInitializer
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

/**
 * This mapper will get the publication title, year and venue as parts of the key and the list of authors as parts of value
 * and if the record only has one author, then it will write the output as venue(key) publication_title(value)
 */
class MyMapper extends Mapper[Text, Text, Text, Text] {
  System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, "src/main/resources/configuration/logback.xml")
  val logger: Logger = LoggerFactory.getLogger(classOf[MyMapper])

  override def map(key: Text, value: Text, context: Mapper[Text, Text, Text, Text]#Context): Unit = {
    if (value.toString.split("` ").length == 1) {
      val venue = extractVenue(key.toString)
      val title = extractPublicationTitle(key.toString)
      logger.info(s"Venue - $venue Title - $title")
      context.write(new Text(venue), new Text(title))
    }
  }

  def extractVenue(key: String): String = key.split("` ")(2).stripPrefix("`").stripSuffix("`")
  def extractPublicationTitle(key: String): String = key.split("` ")(0).stripPrefix("`").stripSuffix("`")
}
