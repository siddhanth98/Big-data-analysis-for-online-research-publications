package hadoop.task2

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class MyMapper extends Mapper[Text, Text, Text, IntWritable] {
  override def map(key: Text, value: Text, context: Mapper[Text, Text, Text, IntWritable]#Context): Unit = {
    val year = extractYear(key)
    if (year.length > 0)
      value.toString.split("` `")
        .foreach(author => context.write(new Text(author.strip().stripPrefix("`").stripSuffix("`")), new IntWritable(year.toInt)))
  }

  def extractYear(key: Text): String = key.toString.split("` `")(1).strip().stripPrefix("`").stripSuffix("`")
}
