package ca.uwaterloo.cs451.project

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.rogach.scallop._
import org.apache.hadoop.fs._

import scala.collection.immutable.{HashMap,HashSet}

object Random {

  class Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(vert, eold, output)
    val vert = opt[String](descr = "vertices file", required = true)
    val eold = opt[String](descr = "old edges file", required = true)
    val output = opt[String](descr = "output path", required = true)
    verify()
  }

  def main(argv: Array[String]) {
    val args = new Conf(argv)
      val conf = new SparkConf().setAppName("Random")
      val sc = new SparkContext(conf)
      FileSystem.get(sc.hadoopConfiguration).delete(new Path(args.output()), true)

      val authorsRDD = sc.textFile(args.vert())
        .map(line => line.split('\t'))
        .map {
          case Array(vertexId, authorName) => (vertexId.toLong, authorName)
        }
      
      val collabOldRDD = sc.textFile(args.eold())
        .map(line => line.split('\t'))
        .map {
          case Array(source, target) => Edge(source.toLong, target.toLong, "collab")
        }
      collabOldRDD.cache()
  
      val connected0 = collabOldRDD
        .map {
          case Edge(source, target, _) => (source, target)
        }
        .collect
        .toSet
      val connected0Broadcast = sc.broadcast(connected0)

      val defaultAuthor = "John Doe"
      val graph0 = Graph(authorsRDD, collabOldRDD, defaultAuthor)
      graph0.cache()

      val rand = scala.util.Random

      val scores = graph0.vertices
        .cartesian(graph0.vertices)
        .filter {
          case ((vertexId0, attr0), (vertexId1, attr1)) => 
            vertexId0 < vertexId1 &&
            !(connected0Broadcast.value.contains((vertexId0, vertexId1)) || connected0Broadcast.value.contains((vertexId1, vertexId0)))
        }
        .map {
          case ((vertexId0, attr0), (vertexId1, attr1)) => {
            ((vertexId0, vertexId1), rand.nextInt)
          }
        }
        .sortBy(_._2, false)
      scores.saveAsTextFile(args.output())
    }
}