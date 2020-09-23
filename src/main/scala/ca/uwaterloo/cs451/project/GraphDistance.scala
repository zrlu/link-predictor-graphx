package ca.uwaterloo.cs451.project

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.rdd.RDD
import org.rogach.scallop._
import org.apache.hadoop.fs._

import scala.collection.immutable.{HashMap,HashSet}

object GraphDistance {

  class Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(vert, eold, output)
    val vert = opt[String](descr = "vertices file", required = true)
    val eold = opt[String](descr = "old edges file", required = true)
    val output = opt[String](descr = "output path", required = true)
    verify()
  }

  def main(argv: Array[String]) {
    val args = new Conf(argv)
      val conf = new SparkConf().setAppName("GraphDistance")
      val sc = new SparkContext(conf)
      FileSystem.get(sc.hadoopConfiguration).delete(new Path(args.output()), true)

      val authorsRDD = sc.textFile(args.vert())
        .map(line => line.split('\t'))
        .map {
          case Array(vertexId, authorName) => (vertexId.toLong, authorName)
        }
      
      val collabOldRDD = sc.textFile(args.eold())
        .map(line => line.split('\t'))
        .flatMap {
          case Array(source, target) => List(
            Edge(source.toLong, target.toLong, "collab"),
            Edge(target.toLong, source.toLong, "collab")
          )
        }
      collabOldRDD.cache()

      val connected0 = collabOldRDD
        .map {
          case Edge(source, target, _) => (source, target)
        }
        .collect
        .toSet
      val connected0Broadcast = sc.broadcast(connected0)

      val allVertexIds = authorsRDD
        .map{ case (vertexId, authorName) => vertexId }
        .collect

      val graph0 = Graph(authorsRDD, collabOldRDD)
      graph0.cache()

      val withGraphDistance = ShortestPaths.run(graph0, allVertexIds)
      val scores = withGraphDistance.vertices
        .flatMap {
          case (vertexId0, spMap) => 
          spMap.filter {
            case (vertexId1, distance) => {
               vertexId0 < vertexId1 &&
               !(connected0Broadcast.value.contains((vertexId0, vertexId1)) || connected0Broadcast.value.contains((vertexId1, vertexId0)))
            }
          }
          .map {
            case (vertexId1, distance) => ((vertexId0, vertexId1), -distance)
          }
        }
        .sortBy(_._2, false)
      scores.saveAsTextFile(args.output())
  }
}