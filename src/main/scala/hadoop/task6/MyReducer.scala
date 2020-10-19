package hadoop.task6

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

class MyReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  override def reduce(key: Text, value: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    val iter = value.iterator()
    context.write(new Text(s"`${key.toString.strip().stripPrefix("`").stripSuffix("`")}`\t"), new IntWritable(getTotalAuthorPublicationCount(iter, 0)))
  }

  def getTotalAuthorPublicationCount(iter: java.util.Iterator[IntWritable], count: Int): Int = {
    var count = 0
    while(iter.hasNext) count += iter.next().get()
    count
  }
}
