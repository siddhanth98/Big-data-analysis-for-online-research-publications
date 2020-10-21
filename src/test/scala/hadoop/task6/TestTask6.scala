package hadoop.task6

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mrunit.mapreduce.{MapDriver, ReduceDriver}
import java.util

import org.junit.{Before, Test}

/**
 * This class will test the mapper, reducer and driver of task6
 */
class TestTask6 {
  val job1Mapper = new Job1Mapper
  val job1Reducer = new Job1Reducer
  val job2Mapper = new Job2Mapper
  val job2Reducer = new Job2Reducer
  var job1MapDriver: MapDriver[Text, Text, Text, IntWritable] = _
  var job1ReduceDriver: ReduceDriver[Text, IntWritable, Text, IntWritable] = _
  var job2MapDriver: MapDriver[LongWritable, Text, IntWritable, Text] = _
  var job2ReduceDriver: ReduceDriver[IntWritable, Text, Text, IntWritable] = _

  private val config = ConfigFactory.parseFile(new File("src/main/resources/configuration/task6Test.conf"))
  val job1MapInputKey: String = config.getString("conf.JOB1.MAPPER.INPUT.KEY")
  val job1MapInputValue: String = config.getString("conf.JOB1.MAPPER.INPUT.VALUE")
  val job1MapOutputKey: String = config.getString("conf.JOB1.MAPPER.OUTPUT.KEY")
  val job1MapOutputValue: Int = config.getInt("conf.JOB1.MAPPER.OUTPUT.VALUE")

  val job1ReducerInput1Key: String = config.getString("conf.JOB1.REDUCER.INPUT1.KEY")
  val job1ReducerInput1Value: util.List[Integer] = config.getIntList("conf.JOB1.REDUCER.INPUT1.VALUE")
  val job1ReducerInput2Key: String = config.getString("conf.JOB1.REDUCER.INPUT2.KEY")
  val job1ReducerInput2Value: util.List[Integer] = config.getIntList("conf.JOB1.REDUCER.INPUT2.VALUE")
  val job1ReducerOutput1Key: String = config.getString("conf.JOB1.REDUCER.OUTPUT1.KEY")
  val job1ReducerOutput1Value: Int = config.getInt("conf.JOB1.REDUCER.OUTPUT1.VALUE")
  val job1ReducerOutput2Key: String = config.getString("conf.JOB1.REDUCER.OUTPUT2.KEY")
  val job1ReducerOutput2Value: Int = config.getInt("conf.JOB1.REDUCER.OUTPUT2.VALUE")

  val job2MapInputKey: Int = config.getInt("conf.JOB2.MAPPER.INPUT.KEY")
  val job2MapInputValue: String = config.getString("conf.JOB2.MAPPER.INPUT.VALUE")
  val job2MapOutputKey: Int = config.getInt("conf.JOB2.MAPPER.OUTPUT.KEY")
  val job2MapOutputValue: String = config.getString("conf.JOB2.MAPPER.OUTPUT.VALUE")

  val job2ReducerInput1Key: Int = config.getInt("conf.JOB2.REDUCER.INPUT1.KEY")
  val job2ReducerInput1Value: util.List[String] = config.getStringList("conf.JOB2.REDUCER.INPUT1.VALUE")
  val job2ReducerInput2Key: Int = config.getInt("conf.JOB2.REDUCER.INPUT2.KEY")
  val job2ReducerInput2Value: util.List[String] = config.getStringList("conf.JOB2.REDUCER.INPUT2.VALUE")
  val job2ReducerOutput1Key: String = config.getString("conf.JOB2.REDUCER.OUTPUT1.KEY")
  val job2ReducerOutput1Value: Int = config.getInt("conf.JOB2.REDUCER.OUTPUT1.VALUE")
  val job2ReducerOutput2Key: String = config.getString("conf.JOB2.REDUCER.OUTPUT2.KEY")
  val job2ReducerOutput2Value: Int = config.getInt("conf.JOB2.REDUCER.OUTPUT2.VALUE")

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
   * This function will test the job1 mapper of task6
   */
  @Test
  def testJob1Mapper(): Unit = {
    job1MapDriver.withInput(new Text(job1MapInputKey), new Text(job1MapInputValue))
    job1MapDriver.withOutput(new Text(job1MapOutputKey), new IntWritable(job1MapOutputValue))
    job1MapDriver.runTest(false)
  }

  /**
   * This function will test the implementation of the job1 reducer of task6
   */
  @Test
  def testJob1Reducer(): Unit = {
    val reduceInput1ValueList: util.List[IntWritable] = new util.ArrayList[IntWritable]()
    val reduceInput2ValueList: util.List[IntWritable] = new util.ArrayList[IntWritable]()
    job1ReducerInput1Value.forEach(e => reduceInput1ValueList.add(new IntWritable(e)))
    job1ReducerInput2Value.forEach(e => reduceInput2ValueList.add(new IntWritable(e)))

    job1ReduceDriver.withInput(new Text(job1ReducerInput1Key), reduceInput1ValueList)
    job1ReduceDriver.withInput(new Text(job1ReducerInput2Key), reduceInput2ValueList)
    job1ReduceDriver.withOutput(new Text(job1ReducerOutput1Key), new IntWritable(job1ReducerOutput1Value))
    job1ReduceDriver.withOutput(new Text(job1ReducerOutput2Key), new IntWritable(job1ReducerOutput2Value))
    job1ReduceDriver.runTest(false)
  }

  /**
   * This function will test the implementation of job2 mapper of task6
   */
  @Test
  def testJob2Mapper(): Unit = {
    job2MapDriver.withInput(new LongWritable(job2MapInputKey), new Text(job2MapInputValue))
    job2MapDriver.withOutput(new IntWritable(job2MapOutputKey), new Text(job2MapOutputValue))
    job2MapDriver.runTest(false)
  }

  /**
   * This function will test the implementation of job2 reducer of task6
   */
  @Test
  def testJob2Reducer(): Unit = {
    val reduceInput1ValueList: util.List[Text] = new util.ArrayList[Text]()
    val reduceInput2ValueList: util.List[Text] = new util.ArrayList[Text]()
    job2ReducerInput1Value.forEach(e => reduceInput1ValueList.add(new Text(e)))
    job2ReducerInput2Value.forEach(e => reduceInput2ValueList.add(new Text(e)))

    job2ReduceDriver.withInput(new IntWritable(job2ReducerInput1Key), reduceInput1ValueList)
    job2ReduceDriver.withInput(new IntWritable(job2ReducerInput2Key), reduceInput2ValueList)
    job2ReduceDriver.withOutput(new Text(job2ReducerOutput1Key), new IntWritable(job2ReducerOutput1Value))
    job2ReduceDriver.withOutput(new Text(job2ReducerOutput2Key), new IntWritable(job2ReducerOutput2Value))
    job2ReduceDriver.runTest(false)
  }
}
