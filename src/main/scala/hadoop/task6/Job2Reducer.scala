package hadoop.task6

import java.lang

import ch.qos.logback.classic.util.ContextInitializer
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
 * This reducer will get the key-value pairs in descending-order sorted by the solo-publication count
 * and will simply write the output same as that of the output.
 */
class Job2Reducer extends Reducer[IntWritable, Text, Text, IntWritable] {
  System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, "src/main/resouces/configuration/logback.xml")
  val logger: Logger = LoggerFactory.getLogger(classOf[Job2Reducer])

  override def reduce(key: IntWritable, values: lang.Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
    val authors = extractAuthorFromIterable(values)
    authors.foreach(a => {
      logger.info(s"Writing author - $a with ${key.toString} publications")
      context.write(new Text(a), key)
    })
  }

  /**
   * This function will collect all aggregated author names into a list and return the list
   * There maybe multiple author names as 2 or more authors may have authored the same number of publication
   * by themselves.
   * @param value The aggregated author names
   */
  def extractAuthorFromIterable(value: java.lang.Iterable[Text]): List[String] = {
    val iter = value.iterator()
    val authorList: ListBuffer[String] = ListBuffer[String]()
    while(iter.hasNext) authorList.append(s"`${iter.next().toString.strip().stripPrefix("`").stripSuffix("`")}`")
    authorList.toList
  }
}
