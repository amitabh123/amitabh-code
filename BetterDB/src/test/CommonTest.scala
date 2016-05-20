package test

import mux.db.config.TraitDBConfig

object CommonTest {
  object MyDBConfig extends TraitDBConfig {
    val dbname:String = "demo"
    val dbhost:String = "localhost"
    val dbms:String = "h2"  // e.g. mysql or h2 or postgresql
    val dbuser:String = "alice"
    val dbpass:String = "password"
    val connTimeOut:Int = 1000 // seconds
    val usePool:Boolean = true // e.g. true (use db pool)
    val configSource = "test"
  }
  def usingExit(f: => Unit) = try {
    f
  } catch {
    case a:Any => a.printStackTrace
  } finally System.exit(1)
}
