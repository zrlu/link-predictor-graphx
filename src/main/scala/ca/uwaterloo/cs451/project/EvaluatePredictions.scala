package ca.uwaterloo.cs451.project

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.rogach.scallop._
import scala.collection.mutable.Map
import scala.collection.immutable.{HashMap,HashSet}

object EvaluatePredictions {

  val kappa_training = 5
  val kappa_testing = 4
  
  class Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(vert, eold, enew, rank)
    val vert = opt[String](descr = "vertices file", required = true)
    val eold = opt[String](descr = "old edges file", required = true)
    val enew = opt[String](descr = "new edges file", required = true)
    val rank = opt[String](descr = "rank edges file", required = true)
    verify()
  }

  def main(argv: Array[String]) {
    val args = new Conf(argv)
    val conf = new SparkConf().setAppName("EvaluatePredictions")
    val sc = new SparkContext(conf)

    val authorsRDD = sc.textFile(args.vert())
      .map(_.split('\t'))
      .map {
        case Array(vertexId, authorName) => vertexId.toLong
      }
    
    val collabOld = sc.textFile(args.eold())
      .map(_.split('\t'))
      .map {
        case Array(source, target) => (source.toLong, target.toLong)
      }
      .collect

    val collabNew = sc.textFile(args.enew())
      .map(_.split('\t'))
      .map {
        case Array(source, target) => (source.toLong, target.toLong)
      }
      .collect

    
    val connected0 = collabOld
      .toSet
    val connected0Broadcast = sc.broadcast(connected0)

    val connected1 = collabNew
      .toSet
    val connected1Broadcast = sc.broadcast(connected1)
    
    val edgesCount0 = Map[Long, Int]()
    val edgesCount1 = Map[Long, Int]()

    collabOld
      .foreach {
        case (source, target) => {
          if (edgesCount0.contains(source)) {
              edgesCount0(source) += 1
          } else {
              edgesCount0(source) = 1
          }
          if (edgesCount0.contains(target)) {
              edgesCount0(target) += 1
          } else {
              edgesCount0(target) = 1
          }
        }
      }
    collabNew
      .foreach {
        case (source, target) => {
          if (edgesCount1.contains(source)) {
              edgesCount1(source) += 1
          } else {
              edgesCount1(source) = 1
          }
          if (edgesCount1.contains(target)) {
              edgesCount1(target) += 1
          } else {
              edgesCount1(target) = 1
          }
        }
      }

    val coreRdd = authorsRDD
      .filter {
        vertexId => (edgesCount0.contains(vertexId) && edgesCount0(vertexId) >= kappa_training && edgesCount1.contains(vertexId) && edgesCount1(vertexId) >= kappa_testing)
      }
    
    val coreSet = coreRdd.collect.toSet
    println("core size="+coreSet.size)
    val coreSetBroadcast = sc.broadcast(coreSet)

    val collabNewStar = collabNew
      .filter {
        case (source, target) => {
          coreSetBroadcast.value.contains(source) && coreSetBroadcast.value.contains(target)
        }
      }
    
    val eNewStarSet = collabNewStar.toSet
    val eNewStarSetBroadcast = sc.broadcast(eNewStarSet)
    val totalSize = eNewStarSetBroadcast.value.size

    val rankRDD = sc.textFile(args.rank() + "/part-[0-9]*")
      .map(line => {
          val tokens = line.replaceAll("\\(|\\)", "").split(",")
          (tokens(0).toLong, tokens(1).toLong)
        }
      )
      .filter {
        case (source, target) => {
          coreSetBroadcast.value.contains(source) && coreSetBroadcast.value.contains(target)
        }
      }
      .take(totalSize)
    
    val truePositiveCount = rankRDD
      .map {
        case (source, target) => {
          if (eNewStarSetBroadcast.value.contains((source, target)) || eNewStarSetBroadcast.value.contains((target, source))) {
            1
          }
          else {
            0
          }
        }
      }
      .reduce(_+_)
    
    val correctness = truePositiveCount.toFloat / totalSize

    println(s"${truePositiveCount.toFloat}/${totalSize}")
    println("correctness=" + correctness)

  }
}