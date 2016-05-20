
package amit.common.file

import amit.common.file.prop._
import java.io.{InputStream => IS}
import java.io.{OutputStream => OS}

trait TraitFilePropertyReader extends TraitCommonFilePropReader {
  def processIS(is:IS):IS = is
  def processOS(os:OS):OS = os
}

trait TraitPlaintextFileProperties extends TraitCommonFilePropReader {
  def processIS(is:IS):IS = is
  def processOS(os:OS):OS = os  
}

class PlaintextFileProperties(val propertyFile:String) extends TraitPlaintextFileProperties {
  initialize
}

