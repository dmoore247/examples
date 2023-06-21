// Databricks notebook source
// MAGIC %md # Fast Vector search / ANN
// MAGIC
// MAGIC Blog Post:https://talks.anghami.com/blazing-fast-approximate-nearest-neighbour-search-on-apache-spark-using-hnsw/
// MAGIC
// MAGIC Github: https://github.com/stepstone-tech/hnswlib-jna
// MAGIC
// MAGIC add library `com.stepstone.search.hnswlib.jna:hnswlib-jna:1.4.2`

// COMMAND ----------

package hnsw

import java.nio.file.Paths

import com.stepstone.search.hnswlib.jna.{Index, SpaceName}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.reflect.runtime.universe.TypeTag


class HnswIndex[DstKey : TypeTag : Encoder](vectorSize: Int,
                                            index: Index,
                                            objectIDsMap: Dataset[(Int, DstKey)]) {

  private val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

  /**
    * Executres KNN query using an HNSW index.
    *
    * @param queryFeatures features to generates recs for
    * @param minScoreThreshold Minimum similarity/distance.
    * @param topK number of top recommendations to generate per instance
    * @param ef HNSW search time parameter
    * @param queryNumPartitions number of partitions for query vectors
    * @return
    */
  def knnQuery[SrcKey : TypeTag : Encoder](queryFeatures: Dataset[(SrcKey, Array[Float])],
                                           minScoreThreshold : Double,
                                           topK: Int,
                                           ef: Int,
                                           queryNumPartitions: Int = 200): Dataset[(SrcKey, DstKey, Double)] = {

    // init tmp directory
    val indexLocalLocation = Paths.get(s"/tmp/vector_recommender/hnsw_index_${System.currentTimeMillis()}")
    val indexFileName = indexLocalLocation.getFileName.toString

    // saving index locally
    index.save(indexLocalLocation)

    // add file to spark context to be sent to running nodes
    spark.sparkContext.addFile(indexLocalLocation.toAbsolutePath.toString)

    // The current interface to HNSW misses the functionality of setting the ef query time
    // parameter, but it's lower bounded by topK as per https://github.com/nmslib/hnswlib/blob/master/ALGO_PARAMS.md#search-parameters,
    // so set a large value of k as max(ef, topK) to get high recall, then cut off after getting the nearest neighbor.
    val k = math.max(topK, ef)

    // local scope vectorSize
    val vectorSizeLocal = vectorSize

    // execute querying
    queryFeatures
      .repartition(queryNumPartitions)
      .toDF("id", "features")
      .withColumn("features", $"features".cast("array<float>"))
      .as[(SrcKey, Array[Float])]
      .mapPartitions((vectors: Iterator[(SrcKey, Array[Float])]) => {
        // load index
        val index = new Index(SpaceName.COSINE, vectorSizeLocal)
        index.load(Paths.get(indexFileName), 0)
        // query vectors
        vectors.flatMap {
          case (idx: SrcKey, vector: Array[Float]) =>
            val queryTuple = index.knnQuery(vector, k)
            queryTuple.getLabels
              .zip(queryTuple.getCoefficients)
              .map(qt => (idx, qt._1, 1.0 - qt._2.toDouble))
              .filter(_._3 >= minScoreThreshold)
              .sortBy(_._3)
              .reverse
              .slice(0, topK)
        }
      })
      .as[(SrcKey, Int, Double)]
      .toDF("src_id", "index_id", "score")
      .join(objectIDsMap.toDF("index_id", "dst_id"), Seq("index_id"))
      .select("src_id", "dst_id", "score")
      .repartition($"src_id")
      .as[(SrcKey, DstKey, Double)]
  }

}
