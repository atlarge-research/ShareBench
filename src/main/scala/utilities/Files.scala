package utilities

object Files {
  def list(path: String): List[String] = {
    val d = new java.io.File(path)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.map(_.getName)
    } else {
      List[String]()
    }
  }
}
