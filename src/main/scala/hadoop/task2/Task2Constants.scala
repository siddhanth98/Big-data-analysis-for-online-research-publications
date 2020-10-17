package hadoop.task2

import java.io.File

import com.typesafe.config.ConfigFactory

object Task2Constants {
  private val config = ConfigFactory.parseFile(new File("src/main/resources/configuration/output_task2.conf"))
  val localOutputPathName: String = config.getString("conf.LOCAL_OUTPUT_DIR_NAME")
}
