package cn.itcast.implic

import java.io.File

/**
  * Created by root on 2016/5/13.
  */
object MyPredef {
  implicit def fileToRichFile(f: File) = new RichFile(f)
}
