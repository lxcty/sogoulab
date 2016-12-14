package lda

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.Vectors

import scala.collection.mutable

/**
  * Created by lx on 9/8/16.
  */
object lda {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDA Modeling").setMaster("local[5]")
    val sc = new SparkContext(conf)


    val iterNum = 100
    val topicNum = 20
    val pathIdArticles = "/home/lx/code/testdata/sogouData/sougouData"
    val corpus = sc.textFile(pathIdArticles)
    val id_news = corpus.map(_.split("\t")).filter(_.length==2).map(x=>(x(0),x(1).split(",")))

    val termCounts = id_news.map(_._2).flatMap(_.map(_ -> 1L)).reduceByKey(_+_).collect().sortBy(-_._2)

    val l = List("c","d","df","dg","e","f","g","h","k","m","mg","mq","o","p","q","r","rg","rr",
      "rz","t","tg","u","ud", "ug","uj","ul","uv","uz","v","vd","vg","vi","vq","x","y","yg","z","zg")
    val vocabArray = termCounts.filter(x=>l.contains(x._1.split(" ")(1))==false).filter(x=>x._1.split(" ")(0).length>1).map(_._1)
    val vocab = vocabArray.zipWithIndex.toMap

    val documents = id_news.map{ case (id, tokens) =>
      val counts = new mutable.HashMap[Int, Double]()
      tokens.foreach { term =>
        if (vocab.contains(term)) {
          val idx = vocab(term)
          counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
        }
      }
      (id.toLong, Vectors.sparse(vocab.size, counts.toSeq))
    }.cache()

    val lda = new LDA().setK(topicNum).setMaxIterations(iterNum)

    val ldaModel = lda.run(documents)
    //val avgLogLikelihood = ldaModel.asInstanceOf(DistributedLDAModel)

    val sampleCount = documents.count()
    val vocabSize = vocab.size
    val avgLogLikelihood = ldaModel.asInstanceOf[DistributedLDAModel].logLikelihood / sampleCount
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 100)


    print("Learned topics (as distributions over vocab of " + ldaModel.vocabSize.toString + " words):\n")
    topicIndices.zipWithIndex.foreach { case ((terms, termWeights), id) =>
      println(s"Topic$id: ")
      var acc = 0.0
      terms.zip(termWeights).foreach { case (term, weight) =>
        acc += weight
        println(s"${vocabArray(term.toInt)} %.3f %.3f".format(weight, acc))
      }
      println()
    }

    println("====================model infor:")
    println("sample size: %s".format(sampleCount))
    println("topic num is: %s".format(topicNum))
    println("iteration times is :%s".format(iterNum))
    println("vocab size:%s".format(vocabSize))
    println("avgLogLikelihood is: %s".format(avgLogLikelihood))
    println("==============================")
  }

}
