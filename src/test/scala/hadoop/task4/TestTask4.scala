package hadoop.task4

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.Text
import org.apache.hadoop.mrunit.mapreduce.{MapDriver, MapReduceDriver, ReduceDriver}
import org.junit.{Before, Test}

/**
 * This class will test the mapper, reducer and driver of task4
 */
class TestTask4 {
  val mapper = new MyMapper
  val reducer = new MyReducer
  var mapDriver: MapDriver[Text, Text, Text, Text] = _
  var reduceDriver: ReduceDriver[Text, Text, Text, Text] = _
  var mapreduceDriver: MapReduceDriver[Text, Text, Text, Text, Text, Text] = _

  private val config = ConfigFactory.parseFile(new File("src/main/resources/configuration/task4Test.conf"))
  val mapInputKey: String = config.getString("conf.MAPPER_INPUT_KEY")
  val mapInputValue: String = config.getString("conf.MAPPER_INPUT_VALUE")
  val mapOutputKey: String = config.getString("conf.MAPPER_OUTPUT_KEY")
  val mapOutputValue: String = config.getString("conf.MAPPER_OUTPUT_VALUE")

  val reduceInputKey: String = config.getString("conf.REDUCER_INPUT_KEY")
  val map1OutputValue: String = config.getString("conf.MAPPER1_OUTPUT_VALUE")
  val map2OutputValue: String = config.getString("conf.MAPPER2_OUTPUT_VALUE")
  val reduceInputValue: util.List[Text] = java.util.List.of(new Text(map1OutputValue), new Text(map2OutputValue))
  val reduceOutputKey: String = config.getString("conf.REDUCER_OUTPUT_KEY")
  val reduceOutputValue: String = config.getString("conf.REDUCER_OUTPUT_VALUE")

  val mapReduceInput1Key: String = config.getString("conf.MAPREDUCE_INPUT1_KEY")
  val mapReduceInput1Value: String =  config.getString("conf.MAPREDUCE_INPUT1_VALUE")
  val mapReduceInput2Key: String = config.getString("conf.MAPREDUCE_INPUT2_KEY")
  val mapReduceInput2Value: String = config.getString("conf.MAPREDUCE_INPUT2_VALUE")
  val mapReduceOutputKey: String = config.getString("conf.MAPREDUCE_OUTPUT_KEY")
  val mapReduceOutputValue: String = config.getString("conf.MAPREDUCE_OUTPUT_VALUE")

  /**
   * This function will initialize the MR Unit Test drivers for the mapper, reducer and driver to be tested using our
   * mappers and reducers
   */
  @Before
  def setup(): Unit = {
    mapDriver = MapDriver.newMapDriver(mapper)
    reduceDriver = ReduceDriver.newReduceDriver(reducer)
    mapreduceDriver = new MapReduceDriver[Text, Text, Text, Text, Text, Text]()
    mapreduceDriver.setMapper(mapper)
    mapreduceDriver.setReducer(reducer)
  }

  /**
   * This function will test the task4 mapper
   */
  @Test
  def testMapper(): Unit = {
    mapDriver.withInput(new Text(mapInputKey), new Text(mapInputValue))
    mapDriver.withOutput(new Text(mapOutputKey), new Text(mapOutputValue))
    mapDriver.runTest(false)
  }

  /**
   * This function will test the task4 reducer
   */
  @Test
  def testReducer(): Unit = {
    reduceDriver.withInput(new Text(reduceInputKey), reduceInputValue)
    reduceDriver.withOutput(new Text(reduceOutputKey), new Text(reduceOutputValue))
    reduceDriver.runTest(false)
  }

  /**
   * This function will test the task4 driver
   */
  @Test
  def testMapReduce(): Unit = {
    mapreduceDriver.withInput(new Text(mapReduceInput1Key), new Text(mapReduceInput1Value))
    mapreduceDriver.withInput(new Text(mapReduceInput2Key), new Text(mapReduceInput2Value))
    mapreduceDriver.withOutput(new Text(mapReduceOutputKey), new Text(mapReduceOutputValue))
    mapreduceDriver.runTest(false)
  }
}
