package hadoop.task3

import java.io.File

import com.typesafe.config.ConfigFactory

object Task3Constants {
  private val config = ConfigFactory.parseFile(new File("src/main/resources/configuration/output_task3.conf"))
  val localOutputPathName: String = config.getString("conf.LOCAL_OUTPUT_DIR_NAME")
}
