package hadoop.task6

import org.apache.hadoop.io.{IntWritable, WritableComparable, WritableComparator}

  class DescendingIntWritableComparable() extends WritableComparator(classOf[IntWritable], true) {
    override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = -1*super.compare(a, b)
  }
