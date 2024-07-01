import QueryTools.DIR_QUERIES
import com.amazonaws.services.s3.model.ObjectMetadata
import com.databricks.spark.sql.perf.tpcds.TPCDS
import org.apache.spark.sql.SparkSession
import utilities.S3

import java.io.ByteArrayInputStream
import scala.io.Source
import data_structures.QueryStat
import utilities.Queries.getQueryWithDate

class QueryTools(spark: SparkSession) extends ShareBench(spark) {

  spark.sql(s"use database ${ShareBench.DB_NAME}")

  def runTPCDS(queryName: String): Unit = {
    val tpcds = new TPCDS(sqlContext = spark.sqlContext)

    val queryToRun = tpcds.tpcds2_4Queries.filter(q => q.name == queryName)
    if (queryToRun.isEmpty) {
      throw new IllegalArgumentException(s"Unknown query name: $queryName\nAvailable queries: ${tpcds.tpcds2_4Queries.map(_.name).mkString(", ")}")
    }

    tpcds.run(queryToRun)
  }

  def runTPCDS(count: Int): Unit = {
    val tpcds = new TPCDS(sqlContext = spark.sqlContext)

    val queries = tpcds.tpcds2_4Queries
    for (i <- 1 to count) {
      val i = (math.random * queries.length).floor.toInt
      val queryToRun = Seq(queries(i))
      tpcds.run(queryToRun)
    }
  }

  def getTimeAndCount(queryName: String, dateRange: String, numRuns: Int = 1, verbose: Boolean = false): Array[(Long, Long)] = {
    val queryText = getQueryWithDate(queryName, DIR_QUERIES, dateRange)

    (0 until numRuns).map(run => {
      if (verbose) println(s"Starting run ${run + 1}/$numRuns...")
      val startTime = System.nanoTime
      val count = spark.sql(queryText).count
      val time = (System.nanoTime - startTime) / 1000000
      (time, count)
    }).toArray
  }

  def getTime(queryName: String, dateRange: String, numRuns: Int = 1): Array[Long] = {

    val queryText = getQueryWithDate(queryName, DIR_QUERIES, dateRange)

    QueryTools.timeNRuns({
      spark.sql(queryText).show(1)
    }, numRuns)
  }

  def collectStats(queryName: String, range: String, numRuns: Int = 1, numWarmups: Int = 10,
                   overwriteExisting: Boolean = false, onlyNewRanges: Boolean = true,
                   bucket: String = QueryTools.S3_BUCKET_QUERIES): Unit = {

    var allQueries = utilities.Files.list(QueryTools.DIR_QUERIES).filter(_.endsWith(".sql")).map(_.stripSuffix(".sql"))
      .sortBy(_.stripPrefix("q").toInt)

    val queries = if (queryName != "all") {
      allQueries.filter(_ == queryName)
    }
    else {
      println(s"Using queries: '${allQueries.mkString("', '")}'")
      allQueries
    }

    if (queries.isEmpty) {
      throw new IllegalArgumentException(s"No queries found for selector $queryName\nAvailable queries: '${allQueries.mkString("', '")}'")
    }

    // read the default and specific date ranges
    val datesSource = Source.fromFile(QueryTools.PATH_DATES)
    val dateRanges = ujson.read(datesSource.getLines.mkString("\n"))
    val rangesDefault = dateRanges("default").arr.map(_.str).toArray
    datesSource.close()

    println(s"Running generic warm up query for $numWarmups iterations...")
    getTimeAndCount("q1", "d_year = 2000 and d_moy = 1", numRuns = numWarmups, verbose = true)

    for (query <- queries) {
      val content = if (!overwriteExisting && s3.doesObjectExist(bucket, s"$query.csv")) {
        println(s"Retrieving existing stats for query '$query' from storage...")
        S3.getObjectAsString(s3, bucket, s"$query.csv")
      }
      else {
        ""
      }

      var results = QueryTools.statsFromCSV(content)
      val ranges: Array[String] = if (range == "all") {
        try{
          dateRanges(query).arr.map(_.str).toArray
        }
        catch {
          case _: NoSuchElementException =>
            println(s"No custom date ranges found for query '$query' - using default.")
            rangesDefault
        }
      }
      else Array(range)

      println(s"Ranges: [${ranges.mkString("'", "', '", "'")}]")

      for (range <- ranges) {
        if (onlyNewRanges && results.exists(_.range == range)) {
          println(s"Result for query '$query' with range '$range' already exists, skipping...")
        }
        else {
          println(s"Starting warm-up run for query '$query' with range '$range'...")
          getTimeAndCount(query, range, numRuns = 2)
          println(s"Starting query '$query' with range '$range'...")
          val result = getTimeAndCount(query, range, numRuns, verbose = true)
          println(s"rows: ${result(0)._2}, avg time: ${result.map(_._1).sum / result.length.toDouble} min time: ${result.map(_._1).min}, max time: ${result.map(_._1).max}")
          results = results ++ result.map(res => QueryStat(range, res._1, res._2))
        }
      }

      // write results back to storage
      val resultsCSV = (QueryStat.CSVHeader +: results.map(_.toCSV)).mkString("\n") + "\n"
      val resultsCSVBytes = resultsCSV.getBytes("UTF-8")
      val metadata = new ObjectMetadata()
      metadata.setContentLength(resultsCSVBytes.length)
      s3.putObject(bucket, s"$query.csv", new ByteArrayInputStream(resultsCSVBytes), metadata)
    }
  }
}

object QueryTools {

  val DIR_QUERIES: String = s"${ShareBench.DIR_PROJECT}/queries"
  val PATH_DATES: String = DIR_QUERIES + "/dates.json"
  val S3_BUCKET_QUERIES: String = "data/query-stats"

  private def timeNRuns(code: => Unit, n: Int = 2): Array[Long] = {
    (0 until n).map(run => {
      val startTime = System.nanoTime
      code
      (System.nanoTime - startTime) / 1000000
    }).toArray
  }

  private def statsFromCSV(csv: String): Array[QueryStat] = {

    if (csv.isEmpty) {
      return Array.empty
    }

    val header = csv.split("\n").head
    val rows = csv.split("\n").tail

    try {
      rows.map(row => QueryStat.fromCSV(row, header))
    }
    catch {
      case e: IllegalArgumentException =>
        println("Error while parsing CSV file: " + e.getMessage)
        println("Ignoring existing file.")
        Array.empty
    }
  }

}
