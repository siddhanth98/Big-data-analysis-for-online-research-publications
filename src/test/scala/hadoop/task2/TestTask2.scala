package hadoop.task2

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mrunit.mapreduce.{MapDriver, MapReduceDriver, ReduceDriver}
import org.junit.{Before, Test}

/**
 * This class tests the mapper, reducer and the overall driver implementations of task2 for correctness
 * Here all values for the task2 mapper, reducer, MR unit test map, reduce and mapreduce drivers are initialized
 * A "var" is used to initialize the drivers as the initialization is to be done in the setup method to be run
 * before all tests are run, and the drivers are to be accessed by all tester methods in the class
 * Values are taken in from the respective configuration file
 */
class TestTask2 {
  val mapper = new MyMapper
  val reducer = new MyReducer
  var mapDriver: MapDriver[Text, Text, Text, IntWritable] = _
  var reduceDriver: ReduceDriver[Text, IntWritable, Text, Text] = _
  var mapreduceDriver: MapReduceDriver[Text, Text, Text, IntWritable, Text, Text] = _

  private val config = ConfigFactory.parseFile(new File("src/main/resources/configuration/task2Test.conf"))
  val mapInputKey: String = config.getString("conf.MAPPER_INPUT_KEY")
  val mapInputValue: String = config.getString("conf.MAPPER_INPUT_VALUE")
  val mapOutput1Key: String = config.getString("conf.MAPPER_OUTPUT1_KEY")
  val mapOutput1Value: Int = config.getInt("conf.MAPPER_OUTPUT1_VALUE")
  val mapOutput2Key: String = config.getString("conf.MAPPER_OUTPUT2_KEY")
  val mapOutput2Value: Int = config.getInt("conf.MAPPER_OUTPUT2_VALUE")

  val reduceInput1Key: String = config.getString("conf.REDUCER_INPUT1_KEY")
  val reduceInput1Value: util.List[Integer] = config.getIntList("conf.REDUCER_INPUT1_VALUE")
  val reduceInput2Key: String = config.getString("conf.REDUCER_INPUT2_KEY")
  val reduceInput2Value: util.List[Integer] = config.getIntList("conf.REDUCER_INPUT2_VALUE")
  val reduceOutputKey: String = config.getString("conf.REDUCER_OUTPUT_KEY")
  val reduceOutputValue: String = config.getString("conf.REDUCER_OUTPUT_VALUE")

  val mapreduceTitles: Array[String] = config.getString("conf.MAPREDUCE_INPUT_TITLES").split(" ")
  val mapreduceYears: Array[String] = config.getString("conf.MAPREDUCE_INPUT_YEARS").split(" ")
  val mapreduceVenue: String = config.getString("conf.MAPREDUCE_INPUT_VENUE")
  val mapreduceAuthor1: String = config.getString("conf.MAPREDUCE_INPUT_AUTHOR1")
  val mapreduceAuthor2: String = config.getString("conf.MAPREDUCE_INPUT_AUTHOR2")
  val mapreduceOutputKey: String = config.getString("conf.MAPREDUCE_OUTPUT_KEY")
  val mapreduceOutputValue: String = config.getString("conf.MAPREDUCE_OUTPUT_VALUE")

  /**
   * This method will initialize the drivers for the MR unit tests
   */
  @Before
  def setup(): Unit = {
    mapDriver = MapDriver.newMapDriver(mapper)
    reduceDriver = ReduceDriver.newReduceDriver(reducer)
    mapreduceDriver = new MapReduceDriver[Text, Text, Text, IntWritable, Text, Text]()
    mapreduceDriver.setMapper(mapper)
    mapreduceDriver.setReducer(reducer)
  }

  /**
   * This method will test the mapper implementation of task2 for its correctness
   * by providing inputs and matching expected output with the observed output
   */
  @Test
  def testMapper(): Unit = {
    mapDriver.withInput(new Text(mapInputKey), new Text(mapInputValue))
    mapDriver.withOutput(new Text(mapOutput1Key), new IntWritable(mapOutput1Value))
    mapDriver.withOutput(new Text(mapOutput2Key), new IntWritable(mapOutput2Value))
    mapDriver.runTest(false)
  }

  /**
   *  This method will test the reducer implementation of task2 for its correctness
   *  by providing inputs and matching expected output with the observed output
   */
  @Test
  def testReducer(): Unit = {
    val reduceInput1IntWritableList: util.List[IntWritable] = new util.ArrayList[IntWritable]()
    val reduceInput2IntWritableList: util.List[IntWritable] = new util.ArrayList[IntWritable]()
    reduceInput1Value.forEach(e => reduceInput1IntWritableList.add(new IntWritable(e)))
    reduceInput2Value.forEach(e => reduceInput2IntWritableList.add(new IntWritable(e)))

    reduceDriver.withInput(new Text(reduceInput1Key), reduceInput1IntWritableList)
    reduceDriver.withInput(new Text(reduceInput2Key), reduceInput2IntWritableList)
    reduceDriver.withOutput(new Text(reduceOutputKey), new Text(reduceOutputValue))
    reduceDriver.runTest(false)
  }

  /**
   * This method tests the overall driver of the task2 mapper and reducer for correctness
   * by providing inputs and matching expected output with the observed output
   */
  @Test
  def testMapReduce(): Unit = {
    for (i <- 0 until 10)
      mapreduceDriver.withInput(new Text(s"`${mapreduceTitles(i)}` `${mapreduceYears(i)}` `$mapreduceVenue`"),
      if (i == 1) new Text(s"`$mapreduceAuthor1`") else new Text(s"`$mapreduceAuthor1` `$mapreduceAuthor2`"))
    mapreduceDriver.withOutput(new Text(mapreduceOutputKey), new Text(mapreduceOutputValue))
    mapreduceDriver.runTest(false)
  }
}
