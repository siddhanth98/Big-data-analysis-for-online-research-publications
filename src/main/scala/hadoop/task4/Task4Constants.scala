package hadoop.task4

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path

object Task4Constants {
  private val config = ConfigFactory.parseFile(new File("src/main/resources/configuration/output_task4.conf"))
  val localOutputPathName: String = config.getString("conf.LOCAL_OUTPUT_DIR_NAME")
  val hdfsOutputPathJob: Path = new Path(config.getString("conf.HDFS_OUTPUT_DIR_NAME_JOB"))
}
