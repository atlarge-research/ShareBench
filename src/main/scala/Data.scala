import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import org.apache.spark.sql.SparkSession

// Adapted from https://github.com/sacheendra/spark-data-generator/blob/main/src/main/scala/Datagen.scala

class Data(spark: SparkSession, val storagePath: String) extends ShareBench(spark){

  private val FORMAT = "parquet"

  def generate(dsdgenPath: String): Unit = {

    val tables = new TPCDSTables(
      spark.sqlContext,
      dsdgenDir = dsdgenPath, // location of dsdgen
      scaleFactor = ShareBench.DB_SCALE_FACTOR.toString,
      useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
      useStringForDate = false) // true to replace DateType with StringType

    tables.genData(
      location = s"${storagePath}/${ShareBench.DB_NAME}",
      format = FORMAT,
      overwrite = true, // overwrite the data that is already there
      partitionTables = true, // create the partitioned fact tables
      clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files.
      filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
      tableFilter = "", // "" means generate all tables
      numPartitions = ShareBench.DB_SCALE_FACTOR) // how many dsdgen partitions to run - number of input tasks.
  }

  def structure(): Unit = {
    // Run:
    val tables = new TPCDSTables(
      spark.sqlContext,
      dsdgenDir = "", // location of dsdgen
      scaleFactor = ShareBench.DB_SCALE_FACTOR.toString,
      useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
      useStringForDate = false) // true to replace DateType with StringType
    val location = s"${storagePath}/${ShareBench.DB_NAME}"
    println("creating database")
    spark.sql(s"create database if not exists ${ShareBench.DB_NAME} location '$location'")
    println("database created")
    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
    tables.createExternalTables(location, FORMAT, ShareBench.DB_NAME, overwrite = true, discoverPartitions = true)
    // Or, if you want to create temporary tables
    // tables.createTemporaryTables(location, format)

    // For CBO only, gather statistics on all columns:
    tables.analyzeTables(ShareBench.DB_NAME, analyzeColumns = true)
  }
}