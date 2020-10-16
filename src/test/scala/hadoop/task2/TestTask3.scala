package hadoop.task2

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.Text
import org.apache.hadoop.mrunit.mapreduce.{MapDriver, MapReduceDriver, ReduceDriver}
import hadoop.task3.MyMapper
import hadoop.task3.MyReducer
import org.junit.{Before, Test}

/**
 * This class will test the mapper, reducer and driver of task3
 */
class TestTask3 {
  val mapper = new MyMapper
  val reducer = new MyReducer
  var mapDriver: MapDriver[Text, Text, Text, Text] = _
  var reduceDriver: ReduceDriver[Text, Text, Text, Text] = _
  var mapReduceDriver: MapReduceDriver[Text, Text, Text, Text, Text, Text] = _

  private val config = ConfigFactory.parseFile(new File("src/main/resources/configuration/task3Test.conf"))
  val mapperInput1Key: String = config.getString("conf.MAPPER_INPUT1_KEY")
  val mapperInput1Value: String = config.getString("conf.MAPPER_INPUT1_VALUE")
  val mapperInput2Key: String = config.getString("conf.MAPPER_INPUT2_KEY")
  val mapperInput2Value: String = config.getString("conf.MAPPER_INPUT2_VALUE")
  val mapperOutputKey: String = config.getString("conf.MAPPER_OUTPUT_KEY")
  val mapperOutputValue: String = config.getString("conf.MAPPER_OUTPUT_VALUE")

  val reducerInputKey: String = config.getString("conf.REDUCER_INPUT_KEY")
  val mapper1Output: String = config.getString("conf.MAPPER1_OUTPUT_VALUE")
  val mapper2Output: String = config.getString("conf.MAPPER2_OUTPUT_VALUE")
  val reducerInputValue: util.List[Text] = java.util.List.of(new Text(mapper1Output), new Text(mapper2Output))
  val reducerOutputKey: String = config.getString("conf.REDUCER_OUTPUT_KEY")
  val reducerOutputValue: String = config.getString("conf.REDUCER_OUTPUT_VALUE")

  val mapreduceInput1Key: String = config.getString("conf.MAPREDUCE_INPUT1_KEY")
  val mapreduceInput1Value: String = config.getString("conf.MAPREDUCE_INPUT1_VALUE")
  val mapreduceInput2Key: String = config.getString("conf.MAPREDUCE_INPUT2_KEY")
  val mapreduceInput2Value: String = config.getString("conf.MAPREDUCE_INPUT2_VALUE")
  val mapreduceOutputKey: String = config.getString("conf.MAPREDUCE_OUTPUT_KEY")
  val mapreduceOutputValue: String = config.getString("conf.MAPREDUCE_OUTPUT_VALUE")

  /**
   * This function will initialize the MR Unit Test drivers for the mapper, reducer and driver to be tested using our
   * mappers and reducers
   */
  @Before
  def setup(): Unit = {
    mapDriver = MapDriver.newMapDriver(mapper)
    reduceDriver = ReduceDriver.newReduceDriver(reducer)
    mapReduceDriver = new MapReduceDriver[Text, Text, Text, Text, Text, Text]()
    mapReduceDriver.setMapper(mapper)
    mapReduceDriver.setReducer(reducer)
  }

  /**
   * This function will test the task3 mapper
   */
  @Test
  def testMapper(): Unit = {
    mapDriver.withInput(new Text(mapperInput1Key), new Text(mapperInput1Value))
    mapDriver.withInput(new Text(mapperInput2Key), new Text(mapperInput2Value))
    mapDriver.withOutput(new Text(mapperOutputKey), new Text(mapperOutputValue))
    mapDriver.runTest(false)
  }

  /**
   * This function will test the task3 reducer
   */
  @Test
  def testReducer(): Unit = {
    reduceDriver.withInput(new Text(reducerInputKey), reducerInputValue)
    reduceDriver.withOutput(new Text(reducerOutputKey), new Text(reducerOutputValue))
    reduceDriver.runTest(false)
  }

  /**
   * This function will test the task3 driver
   */
  @Test
  def testMapReduce(): Unit = {
    mapReduceDriver.withInput(new Text(mapreduceInput1Key), new Text(mapreduceInput1Value))
    mapReduceDriver.withInput(new Text(mapreduceInput2Key), new Text(mapreduceInput2Value))
    mapReduceDriver.withOutput(new Text(mapreduceOutputKey), new Text(mapreduceOutputValue))
    mapReduceDriver.runTest(false)
  }
}
