package hadoop.task6

import java.lang

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.mutable.ListBuffer

class Job2Reducer extends Reducer[IntWritable, Text, Text, IntWritable] {
  override def reduce(key: IntWritable, values: lang.Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
    val authors = extractAuthorFromIterable(values)
    authors.foreach(a => context.write(new Text(a), key))
  }

  def extractAuthorFromIterable(value: java.lang.Iterable[Text]): List[String] = {
    val iter = value.iterator()
    val authorList: ListBuffer[String] = ListBuffer[String]()
    while(iter.hasNext) authorList.append(s"`${iter.next().toString.strip().stripPrefix("`").stripSuffix("`")}`")
    authorList.toList
  }
}
