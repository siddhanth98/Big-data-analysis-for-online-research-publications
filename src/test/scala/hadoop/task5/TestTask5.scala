package hadoop.task5

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mrunit.mapreduce.{MapDriver, ReduceDriver}
import java.util

import org.junit.{Before, Test}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
 * This class will test the mapper, reducer and driver of task5
 */
class TestTask5 {
  val job1Mapper = new Job1Mapper
  val job1Reducer = new Job1Reducer
  val job2Mapper = new Job2Mapper
  val job2Reducer = new Job2Reducer

  var job1MapDriver: MapDriver[Text, Text, Text, Text] = _
  var job1ReduceDriver: ReduceDriver[Text, Text, Text, IntWritable] = _
  var job2MapDriver: MapDriver[LongWritable, Text, IntWritable, Text] = _
  var job2ReduceDriver: ReduceDriver[IntWritable, Text, Text, IntWritable] = _

  private val config = ConfigFactory.parseFile(new File("src/main/resources/configuration/task5Test.conf"))
  val job1MapInputKey: String = config.getString("conf.JOB1.MAPPER_INPUT_KEY")
  val job1MapInputValue: String = config.getString("conf.JOB1.MAPPER_INPUT_VALUE")
  val job1MapOutput1Key: String = config.getString("conf.JOB1.MAPPER_OUTPUT1_KEY")
  val job1MapOutput1Value: String = config.getString("conf.JOB1.MAPPER_OUTPUT1_VALUE")
  val job1MapOutput2Key: String = config.getString("conf.JOB1.MAPPER_OUTPUT2_KEY")
  val job1MapOutput2Value: String = config.getString("conf.JOB1.MAPPER_OUTPUT2_VALUE")
  val job1MapOutput3Key: String = config.getString("conf.JOB1.MAPPER_OUTPUT3_KEY")
  val job1MapOutput3Value: String = config.getString("conf.JOB1.MAPPER_OUTPUT3_VALUE")

  val job1ReducerInputKey: String = config.getString("conf.JOB1.REDUCER_INPUT_KEY")
  val job1ReducerInputValue: util.List[String] = config.getStringList("conf.JOB1.REDUCER_INPUT_VALUE")
  val job1ReducerOutputKey: String = config.getString("conf.JOB1.REDUCER_OUTPUT_KEY")
  val job1ReducerOutputValue: Int = config.getInt("conf.JOB1.REDUCER_OUTPUT_VALUE")

  val job2MapInputKey: Int = config.getInt("conf.JOB2.MAPPER_INPUT_KEY")
  val job2MapInputValue: String = config.getString("conf.JOB2.MAPPER_INPUT_VALUE")
  val job2MapOutputKey: Int = config.getInt("conf.JOB2.MAPPER_OUTPUT_KEY")
  val job2MapOutputValue: String = config.getString("conf.JOB2.MAPPER_OUTPUT_VALUE")

  val job2ReducerInput1Key: Int = config.getInt("conf.JOB2.REDUCER_INPUT1_KEY")
  val job2ReducerInput1Value: util.List[String] = config.getStringList("conf.JOB2.REDUCER_INPUT1_VALUE")
  val job2ReducerInput2Key: Int = config.getInt("conf.JOB2.REDUCER_INPUT2_KEY")
  val job2ReducerInput2Value: util.List[String] = config.getStringList("conf.JOB2.REDUCER_INPUT2_VALUE")
  val job2ReducerInput3Key: Int = config.getInt("conf.JOB2.REDUCER_INPUT3_KEY")
  val job2ReducerInput3Value: util.List[String] = config.getStringList("conf.JOB2.REDUCER_INPUT3_VALUE")
  val job2ReducerOutput1Key: String = config.getString("conf.JOB2.REDUCER_OUTPUT1_KEY")
  val job2ReducerOutput1Value: Int = config.getInt("conf.JOB2.REDUCER_OUTPUT1_VALUE")
  val job2ReducerOutput2Key: String = config.getString("conf.JOB2.REDUCER_OUTPUT2_KEY")
  val job2ReducerOutput2Value: Int = config.getInt("conf.JOB2.REDUCER_OUTPUT2_VALUE")
  val job2ReducerOutput3Key: String = config.getString("conf.JOB2.REDUCER_OUTPUT3_KEY")
  val job2ReducerOutput3Value: Int = config.getInt("conf.JOB2.REDUCER_OUTPUT3_VALUE")

  /**
   * This function will initialize the MR Unit Test drivers for the mapper, reducer and driver to be tested using our
   * mappers and reducers
   */
  @Before
  def setup(): Unit = {
    job1MapDriver = MapDriver.newMapDriver(job1Mapper)
    job1ReduceDriver = ReduceDriver.newReduceDriver(job1Reducer)
    job2MapDriver = MapDriver.newMapDriver(job2Mapper)
    job2ReduceDriver = ReduceDriver.newReduceDriver(job2Reducer)
  }

  /**
   * This function will test the job1 mapper of task5
   */
  @Test
  def testJob1Mapper(): Unit = {
    job1MapDriver.withInput(new Text(job1MapInputKey), new Text(job1MapInputValue))
    job1MapDriver.withOutput(new Text(job1MapOutput1Key), new Text(job1MapOutput1Value))
    job1MapDriver.withOutput(new Text(job1MapOutput2Key), new Text(job1MapOutput2Value))
    job1MapDriver.withOutput(new Text(job1MapOutput3Key), new Text(job1MapOutput3Value))
    job1MapDriver.runTest(false)
  }

  /**
   * This function will test the implementation of the job1 reducer of task5
   */
  @Test
  def testJob1Reducer(): Unit = {
    val reducerInputValueList: util.List[Text] = new util.ArrayList[Text]()
    job1ReducerInputValue.foreach(e => reducerInputValueList.add(new Text(e)))
    job1ReduceDriver.withInput(new Text(job1ReducerInputKey), reducerInputValueList)
    job1ReduceDriver.withOutput(new Text(job1ReducerOutputKey), new IntWritable(job1ReducerOutputValue))
    job1ReduceDriver.runTest(false)
  }

  /**
   * This function will test the implementation of job2 mapper of task5
   */
  @Test
  def testJob2Mapper(): Unit = {
    job2MapDriver.withInput(new LongWritable(job2MapInputKey), new Text(job2MapInputValue))
    job2MapDriver.withOutput(new IntWritable(job2MapOutputKey), new Text(job2MapOutputValue))
    job2MapDriver.runTest(false)
  }

  /**
   * This function will test the implementation of job2 reducer of task5
   */
  @Test
  def testJob2Reducer(): Unit = {
    val reduceInput1ValueList: util.List[Text] = new util.ArrayList[Text]()
    val reduceInput2ValueList: util.List[Text] = new util.ArrayList[Text]()
    val reduceInput3ValueList: util.List[Text] = new util.ArrayList[Text]()

    job2ReducerInput1Value.forEach(e => reduceInput1ValueList.add(new Text(e)))
    job2ReducerInput2Value.forEach(e => reduceInput2ValueList.add(new Text(e)))
    job2ReducerInput3Value.forEach(e => reduceInput3ValueList.add(new Text(e)))

    job2ReduceDriver.withInput(new IntWritable(job2ReducerInput1Key), reduceInput1ValueList)
    job2ReduceDriver.withInput(new IntWritable(job2ReducerInput2Key), reduceInput2ValueList)
    job2ReduceDriver.withInput(new IntWritable(job2ReducerInput3Key), reduceInput3ValueList)

    job2ReduceDriver.withOutput(new Text(job2ReducerOutput1Key), new IntWritable(job2ReducerOutput1Value))
    job2ReduceDriver.withOutput(new Text(job2ReducerOutput2Key), new IntWritable(job2ReducerOutput2Value))
    job2ReduceDriver.withOutput(new Text(job2ReducerOutput3Key), new IntWritable(job2ReducerOutput3Value))

    job2ReduceDriver.runTest()
  }
}
