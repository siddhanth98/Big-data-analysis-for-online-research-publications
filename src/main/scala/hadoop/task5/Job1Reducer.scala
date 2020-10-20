package hadoop.task5

import ch.qos.logback.classic.util.ContextInitializer
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * This reducer will use a hashset to compute the total coAuthor count using the aggregated coAuthors from all mappers
 * for a given author and will write the output as - key = `author` value = `coAuthorCount`
 */
class Job1Reducer extends Reducer[Text, Text, Text, IntWritable] {
  System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, "src/main/resources/configuration/logback.xml")
  val logger: Logger = LoggerFactory.getLogger(classOf[Job1Reducer])

  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {
    val keyToWrite = s"`${key.toString.strip().stripPrefix("`").stripSuffix("`")}`\t"
    val valueToWrite = getTotalCoAuthorCount(values)
    logger.info(s"Author - $keyToWrite Total coAuthor count - ${valueToWrite.toString}")
    context.write(new Text(keyToWrite), new IntWritable(valueToWrite))
  }

  /**
   * This function will use a hashset to maintain the coAuthors for a given author
   * as an author can have the same coAuthor in multiple publications
   * It will keep track of the number of unique coAuthors in a count variable and return it in the end
   * @param values The aggregated coAuthors for an author
   */
  def getTotalCoAuthorCount(values: java.lang.Iterable[Text]): Int = {
    val iter = values.iterator()
    val s = mutable.Set[String]()
    var count = 0

    while(iter.hasNext) {
      val publicationCoAuthors = iter.next().toString.split("` `")
      publicationCoAuthors.foreach(coAuthor => {
        if (!s.contains(coAuthor)) count += 1
        s.add(coAuthor)
      })
    }
    count
  }
}
