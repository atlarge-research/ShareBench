package data_structures

import QuerySubmission.NANOS_IN_MILLI
import org.apache.spark.sql.SparkSession
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}

import utilities.Queries.getQueryWithDate

// TODO: maybe i need to change this to more executors because otherwise the driver blocks new tasks..
import scala.concurrent.ExecutionContext.Implicits.global

case class QuerySubmission(app: String, start: Long, query: String, range: String, dirQueries: String) {

  private val queryText = getQueryWithDate(query, dirQueries, range)

  def runWhenReady(spark: SparkSession, startTime: Long, executionContext: ExecutionContext): Future[QuerySubmissionResult] = {
    // block while its not time yet:
    println(s"Submission for query '$query' at T+$start ms waiting for its time to shine...")
    while (System.currentTimeMillis() - startTime < start) Thread.sleep(100)
    // then spawn the task but already return the future:
    val delay = System.currentTimeMillis - startTime - start
    println(s"Submitting '$query' at T+$start ms ($delay ms delay)...")
    val now = System.nanoTime
    Future(run(spark, now, delay))(executionContext)
  }

  def run(spark: SparkSession, runtimeStart: Long, delay: Long): QuerySubmissionResult = {
    // figure something out to better handle what should be measured...

    // print submission time to check for issues with thread pool:
    println(s"Query '$query' at T+$start ms submitted (${(System.nanoTime - runtimeStart) / NANOS_IN_MILLI} ms after submission was started).")
    spark.sql(queryText).count
    val runtimeTotal = (System.nanoTime - runtimeStart) / NANOS_IN_MILLI
    println(s"Submission for query '$query' at T+$start ms finished in $runtimeTotal ms!")
    QuerySubmissionResult(app, start, query, range, delay, runtimeTotal)
  }
}

object QuerySubmission {
  val NANOS_IN_MILLI = 1000000
}