package ca.uwaterloo.cs451.project

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.rogach.scallop._
import org.apache.hadoop.fs._

import scala.collection.immutable.{HashMap,HashSet}

object PreferentialAttachment {

  class Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(vert, eold, output)
    val vert = opt[String](descr = "vertices file", required = true)
    val eold = opt[String](descr = "old edges file", required = true)
    val output = opt[String](descr = "output path", required = true)
    verify()
  }

  def main(argv: Array[String]) {
    val args = new Conf(argv)
      val conf = new SparkConf().setAppName("PreferentialAttachment")
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

      val graph0 = Graph(authorsRDD, collabOldRDD)
      graph0.cache()
    
      val withNeighboursVertices = graph0
        .aggregateMessages[HashSet[Long]](
          edgeContext => {
            edgeContext.sendToSrc(HashSet(edgeContext.dstId))
            edgeContext.sendToDst(HashSet(edgeContext.srcId))
          },
          (s1, s2) => s1 ++ s2
        )

      val joinedVertices = graph0
        .outerJoinVertices(withNeighboursVertices)((vertexId, authorName, neighbourSet) => neighbourSet.getOrElse(HashSet()))
        .vertices
      joinedVertices.cache()

      val scores = joinedVertices
        .cartesian(joinedVertices)
        .filter {
          case ((vertexId0, neighbourSet0), (vertexId1, neighbourSet1)) => 
            vertexId0 < vertexId1 &&
            !(connected0Broadcast.value.contains((vertexId0, vertexId1)) || connected0Broadcast.value.contains((vertexId1, vertexId0)))
        }
        .map {
          case ((vertexId0, neighbourSet0), (vertexId1, neighbourSet1)) => {
            ((vertexId0, vertexId1), (neighbourSet0.size * neighbourSet1.size))
          }
        }
        .sortBy(_._2, false)
      scores.saveAsTextFile(args.output())
    }
}