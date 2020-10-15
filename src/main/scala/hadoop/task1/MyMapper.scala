package hadoop.task1

import ch.qos.logback.classic.util.ContextInitializer
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

/**
 * This mapper will extract the venue from the key and write it along with the value received as the output
 */
class MyMapper extends Mapper[Text, Text, Text, Text] {
  /*System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, "src/main/resources/configuration/logback.xml")
  val logger: Logger = LoggerFactory.getLogger(classOf[MyMapper])*/

  override def map(key: Text, value: Text, context: Mapper[Text, Text, Text, Text]#Context): Unit = {

    val venue = extractPublicationVenue(key.toString)
//    logger.info(s"Venue - $venue , Authors - $value")
    context.write(new Text(venue), value)
  }
  def extractPublicationVenue(key: String): String = key.split("` ")(2).stripPrefix("`").stripSuffix("`")
}
