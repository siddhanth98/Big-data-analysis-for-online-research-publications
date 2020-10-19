package hadoop.task5

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class Job2Mapper extends Mapper[LongWritable, Text, IntWritable, Text] {
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, IntWritable, Text]#Context): Unit = {
    val author = extractAuthorFromText(value)
    val coAuthorCount = extractCoAuthorCount(value)
    context.write(new IntWritable(coAuthorCount), new Text(author))
  }

  def extractAuthorFromText(value: Text): String = {
    value.toString.split("`\t")(0).strip().stripPrefix("`").stripSuffix("`")
  }

  def extractCoAuthorCount(value: Text): Int = {
    value.toString.split("`\t")(1).strip().toInt
  }
}
