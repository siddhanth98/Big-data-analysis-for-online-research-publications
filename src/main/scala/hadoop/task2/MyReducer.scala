package hadoop.task2

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.mutable

class MyReducer extends Reducer[Text, IntWritable, Text, Text] {
  override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, Text]#Context): Unit =
    if (hasMoreThanTenYears(values)) context.write(new Text(""), key)

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

  def getSet(values: java.lang.Iterable[IntWritable]): mutable.Set[Int] = {
    val iter = values.iterator()
    val s = mutable.Set[Int]()
    while(iter.hasNext) s += iter.next().get()
    s
  }
}
