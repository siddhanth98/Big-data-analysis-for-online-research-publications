package hadoop.task6

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

/**
 * If the publication only has 1 author, only then will this mapper will write its output, which is as:
 * key = `author` value = 1
 */
class Job1Mapper extends Mapper[Text, Text, Text, IntWritable] {
  override def map(key: Text, value: Text, context: Mapper[Text, Text, Text, IntWritable]#Context): Unit = {
    if (value.toString.split("` `").length == 1) {
      val author = extractAuthorFromValue(value)
      if (author.length > 1) context.write(new Text(author), new IntWritable(1))
    }
  }

  def extractAuthorFromValue(value: Text): String =
    value.toString.split("` `")(0).strip().stripPrefix("`").stripSuffix("`")
}
