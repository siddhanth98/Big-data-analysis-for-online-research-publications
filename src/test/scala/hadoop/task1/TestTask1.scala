package hadoop.task1

import java.io.File
import java.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mrunit.mapreduce.{MapDriver, MapReduceDriver, ReduceDriver}
import org.junit.{Before, Test}

/**
 * This class tests the mapper, reducer and the driver program of task1
 */
class TestTask1 {
  val mapper = new MyMapper
  val reducer = new MyReducer
  var mapDriver: MapDriver[Text, Text, Text, Text] = _
  var reduceDriver: ReduceDriver[Text, Text, Text, Text] = _
  var mapreduceDriver: MapReduceDriver[Text, Text, Text, Text, Text, Text] = _
  val config: Config = ConfigFactory.parseFile(new File("src/main/resources/configuration/task1Test.conf"))
  val mapperInputKey: String = config.getString("conf.MAPPER_INPUT_KEY")
  val mapperInputValue: String = config.getString("conf.MAPPER_INPUT_VALUE")
  val mapperOutputKey: String = config.getString("conf.MAPPER_OUTPUT_KEY")
  val mapperOutputValue: String = config.getString("conf.MAPPER_OUTPUT_VALUE")

  val reducerInputKey: String = config.getString("conf.REDUCER_INPUT_KEY")

  val reducerPubln1: String = config.getString("conf.REDUCER_INPUT_PUBLICATION1")
  val reducerPubln2: String = config.getString("conf.REDUCER_INPUT_PUBLICATION2")
  val reducerPubln3: String = config.getString("conf.REDUCER_INPUT_PUBLICATION3")
  val reducerPubln4: String = config.getString("conf.REDUCER_INPUT_PUBLICATION4")
  val reducerPubln5: String = config.getString("conf.REDUCER_INPUT_PUBLICATION5")
  val reducerPubln6: String = config.getString("conf.REDUCER_INPUT_PUBLICATION6")
  val reducerPubln7: String = config.getString("conf.REDUCER_INPUT_PUBLICATION7")
  val reducerPubln8: String = config.getString("conf.REDUCER_INPUT_PUBLICATION8")
  val reducerPubln9: String = config.getString("conf.REDUCER_INPUT_PUBLICATION9")
  val reducerPubln10: String = config.getString("conf.REDUCER_INPUT_PUBLICATION10")
  val reducerPubln11: String = config.getString("conf.REDUCER_INPUT_PUBLICATION11")
  val reducerPubln12: String = config.getString("conf.REDUCER_INPUT_PUBLICATION12")

  val reducerInputValue: util.List[Text] = java.util.List.of(new Text(reducerPubln1), new Text(reducerPubln2), new Text(reducerPubln3),
    new Text(reducerPubln4), new Text(reducerPubln5), new Text(reducerPubln6), new Text(reducerPubln7), new Text(reducerPubln8),
      new Text(reducerPubln9), new Text(reducerPubln10), new Text(reducerPubln11), new Text(reducerPubln12))

  val reducerOutputKey: String = config.getString("conf.REDUCER_OUTPUT_KEY")
  val reducerOutputValue: String = config.getString("conf.REDUCER_OUTPUT_VALUE")

  val mapreduceInput1Key: String = config.getString("conf.MAPREDUCE_INPUT1_KEY")
  val mapreduceInput1Value: String = config.getString("conf.MAPREDUCE_INPUT1_VALUE")
  val mapreduceInput2Key: String = config.getString("conf.MAPREDUCE_INPUT2_KEY")
  val mapreduceInput2Value: String = config.getString("conf.MAPREDUCE_INPUT2_VALUE")
  val mapreduceOutputKey: String = config.getString("conf.MAPREDUCE_OUTPUT_KEY")
  val mapreduceOutputValue: String = config.getString("conf.MAPREDUCE_OUTPUT_VALUE")

  /**
   * This function will initialize the MR Unit Test map, reduce and mapReduce drivers to be tested
   * using our mappers and reducers
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
   * This function will test the task1 mapper
   */
  @Test
  def testMapper(): Unit = {
    mapDriver.withInput(new Text(mapperInputKey), new Text(mapperInputValue))
    mapDriver.withOutput(new Text(mapperOutputKey), new Text(mapperOutputValue))
    mapDriver.runTest(false)
  }

  /**
   * This function will test the task1 reducer
   */
  @Test
  def testReducer(): Unit = {
    reduceDriver.withInput(new Text(reducerInputKey), reducerInputValue)
    reduceDriver.withOutput(new Text(reducerOutputKey), new Text(reducerOutputValue))
    reduceDriver.runTest(false)
  }

  /**
   * This function will test the task1 driver
   */
  @Test
  def testMapReduce(): Unit = {
    mapreduceDriver.withInput(new Text(mapreduceInput1Key), new Text(mapreduceInput1Value))
    mapreduceDriver.withInput(new Text(mapreduceInput2Key), new Text(mapreduceInput2Value))
    mapreduceDriver.withOutput(new Text(mapreduceOutputKey), new Text(mapreduceOutputValue))
    mapreduceDriver.runTest(false)
  }
}
