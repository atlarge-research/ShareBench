package utilities

import scala.io.Source

object Queries {

  def getQueryWithDate(name: String, dir: String, dateRange: String = ""): String = {
    val filename = s"$dir/$name.sql"
    val bufferedSource = Source.fromFile(filename)
    val queryText = bufferedSource.getLines.mkString("\n").replace("$DATERANGE$", dateRange)
    bufferedSource.close()
    queryText
  }
}
