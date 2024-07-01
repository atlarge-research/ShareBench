package data_structures

case class QuerySubmissionResult(app: String, start: Long, query: String, range: String, delay: Long, duration: Long) {
  def toCSV: String = List(app, start, query, range, delay, duration).mkString(",")
}

object QuerySubmissionResult {
  def CSVHeader: String = List("app", "start", "query", "range", "delay", "duration").mkString(",")
}