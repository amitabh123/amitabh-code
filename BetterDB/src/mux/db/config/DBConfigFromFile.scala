package mux.db.config

import amit.common.file.TraitFilePropertyReader

object DefaultDBConfigFromFile extends DBConfigFromFile("db.properties")

class DBConfigFromFile(val propertyFile:String) extends TraitDBConfig with TraitFilePropertyReader {
  val dbname = read ("dbname", "defaultdb")
  val dbhost = read ("dbhost", "localhost")
  val dbms = read ("dbms", "h2") // can be "mysql" or "postgresql"
  val dbuser = read ("dbuser", "defaultuser")
  val dbpass = read ("dbpass", "defaultpass")
  val connTimeOut = read("dbConnTimeOut", 2000)
  val usePool = read("usePool", true)
  val configSource = "file:"+propertyFile
  init
}


