object CountBashUsers {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: scala CountBashUsers <passwd_file_path>")
      System.exit(1)
    }
    
    val passwdFilePath = args(0)
    
    try {
      // Read the file line by line
      val source = scala.io.Source.fromFile(passwdFilePath)
      
      // Count bash users without using complex operations
      var bashUserCount = 0
      for (line <- source.getLines()) {
        if (!line.startsWith("#") && line.trim.nonEmpty) {
          val fields = line.split(":")
          if (fields.length >= 7 && fields(6) == "/bin/bash") {
            bashUserCount += 1
          }
        }
      }
      
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
