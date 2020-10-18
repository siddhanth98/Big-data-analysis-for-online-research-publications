package hadoop.task2

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

/**
 * The mapper will take in the input as "`title` `year` `venue`" (key) - "`Author1` `Author2` ..." (value)
 * and for each author present in the value, it will output - author (key) - year (value)
 */
class MyMapper extends Mapper[Text, Text, Text, IntWritable] {
  override def map(key: Text, value: Text, context: Mapper[Text, Text, Text, IntWritable]#Context): Unit = {
    val year = extractYear(key)
    if (year.length > 0)
      value.toString.split("` `")
        .foreach(author => context.write(new Text(author.strip().stripPrefix("`").stripSuffix("`")), new IntWritable(year.toInt)))
  }

  /**
   * This function will extract the year component of the publication present in the input text file
   * @param key The key of the mapper : "`title` `year` `venue`" (key) - "`Author1` `Author2` ..." (value)
   */
  def extractYear(key: Text): String = key.toString.split("` `")(1).strip().stripPrefix("`").stripSuffix("`")
}
