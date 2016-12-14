package lda


import com.cloudera.datascience.common.XmlInputFormat
import edu.umd.cloud9.collection.wikipedia.WikipediaPage
import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}

import scala.xml.XML
import java.nio.file.{Paths, Files}
import java.io._

/**
  * Created by lx on 8/24/16.
  */
object getRawData {
  def gbk2utf8 = {
    val path = "/home/lx/code/testdata/news_tensite_xml.dat"
    val content = scala.io.Source.fromFile(path, "gbk")
    val fnm = "/home/lx/code/testdata/news_tensite_xml"
    val writer = new PrintWriter(new File(fnm),"UTF-8")
    for (i <- content) writer.write(i)
  }


  def parseXml(xml: String) = {
    try{
    val xmlFile = XML.loadString(xml)

    val urlSeq = xmlFile \ "url"
    val url = urlSeq(0) match  {
      case <url>{x}</url> => x
      case other => ""
    }

    val titleSeq = xmlFile \ "contenttitle"
    val title = titleSeq(0) match  {
      case <contenttitle>{x}</contenttitle> => x
      case other => ""
    }

    val contentSeq = xmlFile \ "content"
    val content = contentSeq(0) match  {
      case <content>{x}</content> => x
      case other => ""
    }
    (url.toString(), title.toString(), content.toString())}
    catch{case ex: Exception => ex.printStackTrace(); ("","","")}
  }


  def main(args: Array[String]): Unit = {
    val spark_conf = new SparkConf().setAppName("LDA Modeling").setMaster("local[5]")
    val sc = new SparkContext(spark_conf)
    val path = "/home/lx/code/testdata/news_tensite_xml"
    val conf = new Configuration()
    conf.set(XmlInputFormat.START_TAG_KEY,"<doc>")
    conf.set(XmlInputFormat.END_TAG_KEY,"</doc>")
    val kvs = sc.newAPIHadoopFile(path, classOf[XmlInputFormat],
      classOf[LongWritable], classOf[Text], conf)
    val rawXml = kvs.map(p => p._2.toString).cache()

    rawXml.map(parseXml).map(x=>(x._1,x._2)).zipWithIndex().map(x=>x._2+"\t"+x._1._1+","+x._1._2)
      .coalesce(10).saveAsTextFile("/home/lx/code/testdata/sogouData/id_url_title")
    rawXml.map(parseXml).map(x=>x._3).zipWithIndex().map(x=>x._2+"\t"+x._1)
      .coalesce(10).saveAsTextFile("/home/lx/code/testdata/sogouData/id_content")
  }
}
