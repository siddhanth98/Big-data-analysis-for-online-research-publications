package hadoop.task5

import java.lang

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.mutable.ListBuffer

class Job2Reducer extends Reducer[IntWritable, Text, Text, IntWritable] {
  override def reduce(key: IntWritable, values: lang.Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit =
    getAuthorsFromIterable(values).foreach(a => context.write(new Text(s"`$a`"), key))

  def getAuthorsFromIterable(values: lang.Iterable[Text]): List[String] = {
    val iter = values.iterator()
    val authorList: ListBuffer[String] = ListBuffer[String]()
    while(iter.hasNext) authorList.append(iter.next().toString)
    authorList.toList
  }
}
