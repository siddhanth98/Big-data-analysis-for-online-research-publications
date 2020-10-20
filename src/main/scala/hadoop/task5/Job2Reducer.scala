package hadoop.task5

import java.lang

import ch.qos.logback.classic.util.ContextInitializer
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
 * This reducer will get the input key value pairs sorted by coAuthorCount in descending order
 * using the custom comparator and will simply write the output as : key = `author` value = `coAuthorCount`
 */
class Job2Reducer extends Reducer[IntWritable, Text, Text, IntWritable] {
  System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, "src/main/resources/configuration.logback.xml")
  val logger: Logger = LoggerFactory.getLogger(classOf[Job2Reducer])

  override def reduce(key: IntWritable, values: lang.Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
    logger.info(s"CoauthorCount = ${key.toString} for author(s) - ${values.iterator().toString}")
    getAuthorsFromIterable(values).foreach(a => context.write(new Text(s"`$a`"), key))
  }

  def getAuthorsFromIterable(values: lang.Iterable[Text]): List[String] = {
    val iter = values.iterator()
    val authorList: ListBuffer[String] = ListBuffer[String]()
    while(iter.hasNext) authorList.append(iter.next().toString)
    authorList.toList
  }
}
