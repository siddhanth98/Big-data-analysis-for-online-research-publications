package hadoop.task1

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class MyMapper extends Mapper[Text, Text, Text, Text] {
  override def map(key: Text, value: Text, context: Mapper[Text, Text, Text, Text]#Context): Unit = context.write(key, value)
}
