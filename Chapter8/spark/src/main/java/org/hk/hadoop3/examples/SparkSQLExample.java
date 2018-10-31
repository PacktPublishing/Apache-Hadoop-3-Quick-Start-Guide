package org.hk.hadoop3.examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class SparkSQLExample {

	public static void main(String[] args) throws Exception {
		final SparkSession sparkSession = SparkSession
			    .builder()
			    .appName("Simple CSV Read")
			    .config("spark.master", "local")
			    .getOrCreate();
		
		// Get DataFrameReader using SparkSession
		DataFrameReader dataFrameReader = sparkSession.read();
		dataFrameReader.option("header", "false");
		Dataset<Row> csvDataFrame = dataFrameReader.csv("input/students.csv");
		csvDataFrame.printSchema();
		// Create view and execute query to convert types as, by default, all columns have string types
		csvDataFrame.createOrReplaceTempView("STUDENTS_INFO");
				final Dataset<Row> studentsInfo = sparkSession
						.sql("SELECT CAST(srno as int) srno, "
								+ "CAST(name as string) name, "
								+ "CAST(gender as string) gender, "
								+ "CAST(dept_id as string) dept_id, FROM STUDENTS_INFO ");				
		studentsInfo.printSchema();
		//now create another view
		studentsInfo.createOrReplaceTempView("STUDENTS_VIEW");
		sparkSession.sql("SELECT * FROM STUDENTS_VIEW WHERE dept_id = 1").show();
		sparkSession.stop();
	}

}
