package hadoop.task6

import ch.qos.logback.classic.util.ContextInitializer
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

/**
 * This reducer will take in the aggregated list of 1's from all mappers for an author, and will sum all 1's for the author
 * and will write the output as : key = `author` value = total_solo_publication_count
 */
class Job1Reducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, "src/main/resources/configuration/logback.xml")
  val logger: Logger = LoggerFactory.getLogger(classOf[Job1Reducer])

  override def reduce(key: Text, value: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    val iter = value.iterator()
    val keyToWrite = s"`${key.toString.strip().stripPrefix("`").stripSuffix("`")}`\t"
    val count = getTotalAuthorPublicationCount(iter, 0)
    logger.info(s"Author $keyToWrite has $count solo-publications")
    context.write(new Text(keyToWrite), new IntWritable(count))
  }

  /**
   * This function will simply iterate over all values i.e. 1's each of which represents 1 solo publication for that author
   * and will return the total number of 1's, representing the total number of solo publications for that author
   * @param iter The aggregated 1's for an author
   */
  def getTotalAuthorPublicationCount(iter: java.util.Iterator[IntWritable], count: Int): Int = {
    var count = 0
    while(iter.hasNext) count += iter.next().get()
    count
  }
}
