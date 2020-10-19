package hadoop.task5

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.mutable

class Job1Reducer extends Reducer[Text, Text, Text, IntWritable] {
  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, IntWritable]#Context): Unit =
    context.write(new Text(s"`${key.toString.strip().stripPrefix("`").stripSuffix("`")}`\t"), new IntWritable(getTotalCoAuthorCount(values)))

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
