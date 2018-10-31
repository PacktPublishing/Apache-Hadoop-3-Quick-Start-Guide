package org.hk.hadoop3.examples;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkMLExample {

	public static void main(String[] args) throws Exception {

		SparkSession sparkSession = SparkSession.builder().appName("Simple CSV Read").config("spark.master", "local")
				.getOrCreate();

		// Get DataFrameReader using SparkSession
		DataFrameReader dataFrameReader = sparkSession.read();
		dataFrameReader.option("header", "true");
		Dataset<Row> dataset = dataFrameReader.csv("input/computer-stats.csv");
		dataset.printSchema();
		for (String column : dataset.columns())
			dataset = dataset.withColumnRenamed(column, column.replaceAll("[^a-zA-Z0-9]", " "));
		
		dataset.createOrReplaceTempView("COMPUTER_TABLE");
		Dataset<Row> computerInfo = 
				sparkSession.sql("SELECT CAST(srno as int) srno, "
								+ "CAST(price as int) price, "
								+ "CAST(speed as int) speed, "
								+ "CAST(hd as int) hd, "
								+ "CAST(ram as int) ram, "
								+ "CAST(cd as string) cd, "
								+ "CAST(multi as string) multi, "
								+ "CAST(premium as string) premium, "
								+ "CAST(ads as int) ads, "
								+ "CAST(trend as int) trend, "
								+ " FROM COMPUTER_TABLE ");
		computerInfo.printSchema();
		
		computerInfo.createOrReplaceTempView("COMPUTER_VIEW");
		Dataset<Row> sqlDF = sparkSession.sql("SELECT * FROM COMPUTER_TABLE");
		sqlDF.show();
		
		// Trains a k-means model.
		KMeans kmeans = new KMeans().setK(3).setSeed(1L);
		KMeansModel model = kmeans.fit(sqlDF);

		// Make predictions
		Dataset<Row> predictions = model.transform(dataset);

		// Evaluate clustering by computing Silhouette score
		ClusteringEvaluator evaluator = new ClusteringEvaluator();

		double silhouette = evaluator.evaluate(predictions);
		System.out.println("Silhouette score= " + silhouette);

		// Shows the result.
		Vector[] centers = model.clusterCenters();
		System.out.println("Cluster Centers: ");
		for (Vector center : centers) {
			System.out.println(center);
		}
		// $example off$

		sparkSession.stop();
	}

}
