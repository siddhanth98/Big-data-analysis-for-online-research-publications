package hadoop.task6

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

/**
 * This mapper will take in the output of job1 reducer and will write its output as :
 * key = `solo-publication-count` value = `author-name`
 */
class Job2Mapper extends Mapper[LongWritable, Text, IntWritable, Text] {
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, IntWritable, Text]#Context): Unit =
    context.write(new IntWritable(value.toString.split("`\t")(1).strip().stripPrefix("`").stripSuffix("`").toInt),
    new Text(value.toString.split("`\t")(0).strip().stripPrefix("`").stripSuffix("`")))
}
