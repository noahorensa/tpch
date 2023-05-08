import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark {

  val APP_NAME = "Spark-TPC-Benchmark"
  val DISABLE_SPARK_LOGS = true

  lazy val spark: SparkSession = {

    java.util.logging.Logger.getLogger(APP_NAME)
      .info(
        s"\n" +
          s"${"*" * 80}\n" +
          s"${"*" * 80}\n" +
          s"**${" " * 76}**\n" +
          s"**${" " * (38 - APP_NAME.length / 2)}$APP_NAME${" " * (38 + APP_NAME.length / 2 - APP_NAME.length)}**\n" +
          s"**${" " * 76}**\n" +
          s"${"*" * 80}\n" +
          s"${"*" * 80}\n"
      )

    if (DISABLE_SPARK_LOGS) {
      Logger.getLogger("org").setLevel(Level.WARN)
      Logger.getLogger("akka").setLevel(Level.WARN)
    }

    SparkSession.builder()
      .config("spark.ui.showConsoleProgress", true)
//      .master("local[*]")
//      .config("spark.hadoop.javax.jdo.option.ConnectionURL","jdbc:derby:;databaseName=metastore_db;create=true")
//      .config("spark.hadoop.javax.jdo.option.ConnectionDriverName","org.apache.derby.jdbc.EmbeddedDriver")
      .appName(APP_NAME)
//      .enableHiveSupport()
      .getOrCreate()
  }

  def sql: String => DataFrame = spark.sql

  def sc: SparkContext = spark.sparkContext
}
