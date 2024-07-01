package data_structures

case class QueryStat(range: String, time: Long, rows: Long) {
  def toCSV: String = {
    s"$range,$time,$rows"
  }
}

object QueryStat {
  private val HDR_RANGE = "range"
  private val HDR_TIME = "time"
  private val HDR_ROWS = "rows"

  private val HEADER: Array[String] = Array(
    HDR_RANGE, HDR_TIME, HDR_ROWS
  )

  def fromCSV(row: String, header: String): QueryStat = {

    val headerFields = header.split(",").map(_.trim)

    if (!HEADER.sameElements(headerFields)) {
      throw new IllegalArgumentException(
        s"Invalid header. Expected header '${HEADER.mkString(", ")}' but found: '${headerFields.mkString(", ")}'}"
      )
    }

    val fields = row.split(",")
    val range = fields(HEADER.indexOf(HDR_RANGE))
    val time = fields(HEADER.indexOf(HDR_TIME)).toLong
    val rows = fields(HEADER.indexOf(HDR_ROWS)).toLong
    QueryStat(range, time, rows)
  }

  def CSVHeader: String = {
    HEADER.mkString(",")
  }
}