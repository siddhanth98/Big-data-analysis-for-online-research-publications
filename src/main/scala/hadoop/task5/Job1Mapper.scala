package hadoop.task5

import ch.qos.logback.classic.util.ContextInitializer
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

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

  def getCoAuthors(authors: List[String]): mutable.Map[String, List[String]] = {
    val coAuthorMap: mutable.Map[String, List[String]] = mutable.Map[String, List[String]]()
    for (i <- authors.indices)
      coAuthorMap += (authors(i).strip().stripPrefix("`").stripSuffix("`") -> (authors.slice(0, i) ::: authors.slice(i+1, authors.size)))
    coAuthorMap
  }
}
