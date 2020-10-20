package hadoop.task5

import ch.qos.logback.classic.util.ContextInitializer
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * Given key = `title` `year` `venue` and value = `author1` `author2` `author3` ...
 * for each author present in the value text, this mapper will write the author as the key and all the other coAuthor names
 * as the corresponding value text in its output
 */
class Job1Mapper extends Mapper[Text, Text, Text, Text] {
  System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, "src/main/resources/configuration/logback.xml")
  val logger: Logger = LoggerFactory.getLogger(classOf[Job1Mapper])

  override def map(key: Text, value: Text, context: Mapper[Text, Text, Text, Text]#Context): Unit = {
    logger.info(s"got these authors as the values - ${value.toString}\n")

    val authorList = value.toString.split("` `").toList.map(e => e.strip().stripPrefix("`").stripSuffix("`"))
    val coAuthors = getCoAuthors(authorList)
    logger.info(s"got the author map as follows:")
    coAuthors.keySet.foreach(a => logger.info(s"$a -> ${coAuthors.get(a).mkString(" ")}\n"))
    authorList.foreach(a => context.write(new Text(a), new Text(coAuthors.get(a).mkString(" "))))
  }

  /**
   * This function will compute a hashmap of author to coAuthors for each author and return it
   * A mutable map is used to avoid running into stack overflow errors for large number of authors for a publication
   * because of a recursive approach
   */
  def getCoAuthors(authors: List[String]): mutable.Map[String, List[String]] = {
    val coAuthorMap: mutable.Map[String, List[String]] = mutable.Map[String, List[String]]()
    for (i <- authors.indices)
      coAuthorMap += (authors(i).strip().stripPrefix("`").stripSuffix("`") -> (authors.slice(0, i) ::: authors.slice(i+1, authors.size)))
    coAuthorMap
  }
}
