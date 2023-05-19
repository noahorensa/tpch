import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, DataFrameReader, SaveMode}

import java.io.{File, PrintWriter}
import scala.collection.mutable

abstract class TPCBenchmark {

  val schemaName: String
  val tablesDefs: Map[String, Array[StructField]]
  var tables: Map[String, DataFrame] = Map()

  private def csvReaders: Map[String, DataFrameReader] =
    tablesDefs.map{case (name, schema) =>
      name -> Spark.spark.read.format("csv")
        .option("header", "false")
        .option("delimiter", "|")
        .schema(StructType(schema))
    }

  def loadCsv(path: String) {
    csvReaders.foreach{case (name, reader) =>
      reader.load(path + File.separator + name)
        .createOrReplaceTempView(name)
    }
  }

  def writeParquet(path: String) {
//    Spark.spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "none");
//    Spark.spark.sqlContext.setConf("parquet.enable.dictionary", "false");

    val start = System.nanoTime()

    csvReaders.foreach{case (name, reader) =>
      reader.load(path + File.separator + name)
        .write
        .mode(SaveMode.Ignore)
        .parquet(path + File.separator + "parquet" + File.separator + name)
    }

    val end = System.nanoTime()

    println(s"\nParquet ingest completed in ${(end - start) / 1e9}")
  }

  def loadParquet(path: String) {
    tablesDefs.foreach{case (name, _) =>
      val df = Spark.spark.read
        .parquet(path + File.separator + "parquet" + File.separator + name)
      tables = tables + (name -> df)
      df.createOrReplaceTempView(name)
    }
  }

//  def hbaseCatalog(name: String): String ={
//
//    def hbaseType(t: DataType) = {
//      t match {
//        case ByteType => "tinyint"
//        case IntegerType => "int"
//        case LongType => "bigint"
//        case DateType => "string"
//        case _ => t.typeName
//      }
//    }
//
//    s"""{
//       |  "table": {"namespace": "$schemaName", "name": "$name", "tableCoder": "PrimitiveType"},
//       |  "rowkey": "key",
//       |  "columns": {
//       |    "col0": {"cf": "rowkey", "col": "key", "type": "string"},
//       |${tablesDefs(name).zipWithIndex.map(col =>
//          s"""    "col${col._2 + 1}": {"cf": "cf${col._2 + 1}", "col": "${col._1.name}", "type": "${hbaseType(col._1.dataType)}"}"""
//            .stripMargin
//        ).mkString(",\n")
//        }
//       |  }
//       |}""".stripMargin
//  }


//  def writeHbase(path: String) {
//
//    val start = System.nanoTime()
//
//    csvReaders.foreach{case (name, reader) =>
//
//      val catalog = hbaseCatalog(name)
//
//      println(s"Writing table\n$catalog")
//
//      reader.load(path + File.separator + name)
//        .withColumn("key", row_number.over(Window.partitionBy(lit(1)).orderBy(lit(1))))
//        .write
//        .format("org.apache.hadoop.hbase.spark")
//        .options(
//          Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "9")
//        )
//        .option("hbase.spark.use.hbasecontext", false)
//        .save()
//    }
//
//    val end = System.nanoTime()
//
//    println(s"HBase ingest completed in ${(end - start) / 1e9}")
//  }

  val queries: List[(String, String)]

  def initHive(): Unit = {
    Spark.sql(s"use $schemaName")
  }

  def run(outFile: String, outputDir: String) = {

    val times: mutable.MutableList[(String, Double)] = mutable.MutableList()
    val pw = new PrintWriter(new File(outFile))

    queries.foreach(q => {

      try {
        val start = System.nanoTime()

        Spark.sql(q._2)
          .write
          .format("csv")
          .mode(SaveMode.Overwrite)
          .save(s"$outputDir/output/${q._1}")

        val end = System.nanoTime()

        val t = (end - start) / 1e9

        pw.write(s"${q._1} $t\n")
        pw.flush()

        times += ((q._1, t))
      }
      catch {
        case e: Exception =>
          pw.write(s"${q._1}\n\n")
          e.printStackTrace(pw)
          e.printStackTrace()
          pw.write("\n\n")
          pw.flush()
      }
    })

    pw.close()

    println()
    times.foreach{case (name, time) => println(s"$name $time")}
  }
}
