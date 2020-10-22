package hadoop.task4

import ch.qos.logback.classic.util.ContextInitializer
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

/**
* The mapper will take in the key value input as "`title` `year` `venue`(key) : `author1` `author2` `author3` (value)"
 * For the given venue it will output - `venue` (key) : `title number of authors for this title`
*/
class MyMapper extends Mapper[Text, Text, Text, Text] {
  System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, "src/main/resources/configuration/logback.xml")
  val logger: Logger = LoggerFactory.getLogger(classOf[MyMapper])

  override def map(key: Text, value: Text, context: Mapper[Text, Text, Text, Text]#Context): Unit = {
    val venue = extractVenue(key)
    val title = extractPublicationTitle(key)
    val authorCount = value.toString.split("` `").length
    logger.info(s"Writing key = `$venue` value = `$title $authorCount`")
    context.write(new Text(venue), new Text(s"`$title\t$authorCount` "))
  }
  def extractPublicationTitle(key: Text): String = key.toString.split("` `")(0).strip().stripPrefix("`").stripSuffix("`")
  def extractVenue(key: Text): String = "`".concat(key.toString.split("` `")(2).stripPrefix("`").stripSuffix("`")).concat("` ")
}
