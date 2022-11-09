from operator import add
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.clustering import KMeans, BisectingKMeans
import json, random, subprocess, time, sys

def main(input, output, k_class=10000, max_iter=20, distance_measure='euclidean', bisecting=False, dim=64, debug_task='', **kwargs):
    if debug_task:
        output = output.split('/')
        output.insert(-1, debug_task)
        output = '/'.join(output)
    print("output: %s" % output)
    spark = SparkSession \
            .builder \
            .enableHiveSupport() \
            .getOrCreate()
    df = spark.read.json(input)
    df = df.rdd.map(lambda x: [x.id, Vectors.dense(x.emb[:dim]), x.extra]).filter(lambda x: x[1].numNonzeros()).toDF(['label', 'features', 'extra'])

    kmeans_instance = BisectingKMeans() if bisecting else KMeans()
    kmeans = kmeans_instance.setSeed(2).setK(k_class).setMaxIter(max_iter).setDistanceMeasure(distance_measure)
    model = kmeans.fit(df)

    #save pred
    transformed = model.transform(df).select("label", "features", "prediction", "extra")
    transformed.write.mode('overwrite').json(output + '/pred')

    #save model
    model.write().overwrite().save(output + '/model')

    #show the results
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)
    spark.stop()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-k", "--k_class", type=int, default=10000)
    parser.add_argument("-m", "--max_iter", type=int, default=20)
    parser.add_argument("-d", "--distance_measure", type=str, default='euclidean')
    parser.add_argument("-b", "--bisecting", action="store true")