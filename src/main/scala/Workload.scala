import org.apache.spark.sql.SparkSession

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration
import scala.io.Source
import utilities.S3
import data_structures.{QuerySubmission, QuerySubmissionResult}

import java.util.concurrent.Executors

class Workload(spark: SparkSession, querySubmissions: List[QuerySubmission], name: String = "nA") extends ShareBench(spark) {

  def run(app: String, startTime: Long): Unit = {
    spark.sql(s"use database ${ShareBench.DB_NAME}")
    val appSubmissions = querySubmissions.filter(_.app == app)
    val threadPool = Executors.newFixedThreadPool(Workload.NUM_THREADS)
    val executionContext = ExecutionContext.fromExecutor(threadPool)

    // note: this assumes the queries are sorted by time (it's blocking)
    val resultFutures = appSubmissions.map(_.runWhenReady(spark, startTime, executionContext))

    val results = resultFutures.map(Await.result(_, Duration.Inf))
    threadPool.shutdownNow()
    println(s"thread pool status is: ${threadPool.isShutdown}")
    val resultsBucket = s"${Workload.S3_BUCKET}/$name/${startTime / 1000}"
    val csv = (QuerySubmissionResult.CSVHeader :: results.map(_.toCSV)).mkString("\n") + "\n"
    S3.putStringAsObject(s3, resultsBucket, s"$app.csv", csv)
    println("run finished")
    saveLog(app, startTime)
  }

  private def saveLog(app: String, startTime: Long): Unit = {
    val bufferedSource = Source.fromFile(Workload.PATH_LOG)
    val log = bufferedSource.getLines.mkString("\n")
    bufferedSource.close()
    val logsBucket = s"${Workload.S3_BUCKET_LOGS}/$name/${startTime / 1000}"
    val log_extended = log + s"\n${System.currentTimeMillis} WORKLOAD_END\n"
    S3.putStringAsObject(s3, logsBucket, s"$app.log", log_extended)
  }
}

object Workload {
  private val DIR_WORKLOADS = s"${ShareBench.DIR_PROJECT}/workloads"
  private val DIR_QUERIES = s"${ShareBench.DIR_PROJECT}/queries"
  private val S3_BUCKET = "data/workload-traces"
  private val S3_BUCKET_LOGS = "data/dynalloc-logs"

  private val PATH_LOG = s"${ShareBench.DIR_PROJECT}/logs/log4j2.log"

  private val HDR_APP = "app"
  private val HDR_START = "start"
  private val HDR_QUERY = "query"
  private val HDR_RANGE = "range"

  private val NUM_THREADS = 30 // Upper limit for concurrent queries

  private val COLUMNS = Set(
    HDR_APP, HDR_START, HDR_QUERY, HDR_RANGE
  )

  def fromFile(spark: SparkSession, filename: String): Workload = {
    val bufferedSource = Source.fromFile(s"$DIR_WORKLOADS/$filename.csv")
    val lines:List[String] = bufferedSource.getLines().toList.map(_.trim)
    bufferedSource.close()

    val header = lines.head.split(",")
    if (!COLUMNS.forall(header.contains)) {
      throw new IllegalArgumentException(
        s"Invalid workload file. Expected headers: {${COLUMNS.mkString(", ")}} but missing: ${COLUMNS.filterNot(header.contains)}"
      )
    }

    val querySubmissions = lines.tail.map(line => {
      val fields = line.split(",").map(_.trim)
      QuerySubmission(
        app = fields(header.indexOf(HDR_APP)),
        start = fields(header.indexOf(HDR_START)).toLong,
        query = fields(header.indexOf(HDR_QUERY)),
        range = fields(header.indexOf(HDR_RANGE)),
        dirQueries = DIR_QUERIES)

    })

    new Workload(spark, querySubmissions, filename)
  }
}
