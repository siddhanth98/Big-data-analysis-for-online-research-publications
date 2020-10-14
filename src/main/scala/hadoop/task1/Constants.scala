package hadoop.task1

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

object Constants {
  private val config: Config = ConfigFactory.parseFile(new File("src/main/resources/inputs/configuration/task1.conf"))
  val numInputs: Int = config.getInt("conf.NUM_INPUT_FILES")
  val hdfsOutputPath: Path = new Path("/output")
  val localInputPathName: String = config.getString("conf.LOCAL_INPUT_DIR_NAME")
  val localOutputPathName: String = config.getString("conf.LOCAL_OUTPUT_DIR_NAME")
}
