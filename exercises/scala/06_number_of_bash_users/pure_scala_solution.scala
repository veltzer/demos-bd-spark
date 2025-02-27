import scala.io.Source

object CountBashUsers {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: scala CountBashUsers <passwd_file_path>")
      System.exit(1)
    }
    
    val passwdFilePath = args(0)
    
    try {
      // Open the file
      val source = Source.fromFile(passwdFilePath)
      
      // Count bash users
      val bashUserCount = source
        .getLines()
        .filter(line => !line.startsWith("#") && line.trim.nonEmpty) // Skip comments and empty lines
        .count(line => line.split(":").length >= 7 && line.split(":")(6) == "/bin/bash")
      
      // Close the file
      source.close()
      
      // Print result
      println(s"Number of users with /bin/bash as their shell: $bashUserCount")
      
    } catch {
      case e: Exception => 
        println(s"Error processing file: ${e.getMessage}")
        System.exit(1)
    }
  }
}
