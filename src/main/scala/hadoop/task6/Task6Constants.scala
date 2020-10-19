package hadoop.task6

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path

object Task6Constants {
  private val config = ConfigFactory.parseFile(new File("src/main/resources/configuration/output_task6.conf"))
  val localOutputPathName: String = config.getString("conf.LOCAL_OUTPUT_DIR_NAME")
  val hdfsFinalOutputPath: Path = new Path(config.getString("conf.HDFS_FINAL_OUTPUT_DIR_NAME"))
}
