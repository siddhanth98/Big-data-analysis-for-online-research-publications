package hadoop

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

object Constants {
  private val config: Config = ConfigFactory.parseFile(new File("src/main/resources/configuration/task.conf"))
  val numInputs: Int = config.getInt("conf.NUM_INPUT_FILES")
  val hdfsOutputPath: Path = new Path("/output")
  val localInputPathName: String = config.getString("conf.LOCAL_INPUT_DIR_NAME")
}
