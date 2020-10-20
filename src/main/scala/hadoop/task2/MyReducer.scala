package hadoop.task2

import ch.qos.logback.classic.util.ContextInitializer
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * The reducer will take in the input as : "author" (key) - 2009 2010 2017 2009 2005 ....(collected values from all mappers)
 * Then it will sort the list of unique years found in the value, traverse the list and if it finds a contiguous sequence
 * of length >= 10 for the years, then it will output that author otherwise it would not output anything
 */
class MyReducer extends Reducer[Text, IntWritable, Text, Text] {
  System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, "src/main/resources/configuration/logback.xml")
  val logger: Logger = LoggerFactory.getLogger(classOf[MyReducer])

  override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, Text]#Context): Unit =
    if (hasMoreThanTenYears(values)) {
      logger.info(s"Writing author - ${key.toString}")
      context.write(new Text(""), new Text(s"`${key.toString}`"))
    }

  /**
   * This function will convert the given iterable of values into a set to remove the duplicate years as a single author
   * may have published more than once in the same year.
   * Then it will convert this set into a list, sort it in non-decreasing order
   * Then it will traverse the list from the start, and if it finds a contiguous sequence of length >= 10 then it will
   * return true otherwise false
   * @param values The iterable of values collected from all mappers
   */
  def hasMoreThanTenYears(values: java.lang.Iterable[IntWritable]): Boolean = {
    val l = getSet(values).toList.sortBy(e => e)
    if (l.size < 10) return false
    var currentSequenceLength = 1
    var largestSequenceLength = 0

    for (i <- 0 until l.size-1) {
      if (i == l.size-2 && (l(i+1)-l(i) == 1) && currentSequenceLength >= 9) return true
      else if (l(i+1)-l(i) == 1) currentSequenceLength += 1
      else {
        largestSequenceLength = Integer.max(currentSequenceLength, largestSequenceLength)
        if (largestSequenceLength >= 10) return true
        currentSequenceLength = 1
      }
    }
    false
  }

  /**
   * Insert the values in the iterable into a mutable set and return the set
   * Note that a mutable set is used here
   * If an immutable set were to be used then a recursive approach would be required to get all
   * values, which may possibly run into a stack overflow error if there is a large number of publications by the author
   * @param values The iterable of values collected from all mappers
   */
  def getSet(values: java.lang.Iterable[IntWritable]): mutable.Set[Int] = {
    val iter = values.iterator()
    val s = mutable.Set[Int]()
    while(iter.hasNext) s += iter.next().get()
    s
  }
}
