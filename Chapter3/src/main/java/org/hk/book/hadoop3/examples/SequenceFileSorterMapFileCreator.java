package org.hk.book.hadoop3.examples;
import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;



/**
	Copyright [2018] [Hrishikesh Karambelkar]
	
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	
	    http://www.apache.org/licenses/LICENSE-2.0
	
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
**/
/**
 * This is a sequence file sorting class, we also create a map file out of this class.
 * @author hrishikesh.k
 *
 */

public class SequenceFileSorterMapFileCreator {
	/**
	 * This is a mapper class for reading the file information and put it in sequences
	 * 
	 * @author hrishikesh.k
	 *
	 */
	public static class ReadFileMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//simply write the text as it is
			context.write(key, value);
		}
	}

	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) 
			throws Exception {
		Configuration conf = new Configuration();
		
		if (args.length < 2) {
			System.err.println("Usage: SequenceFileSorterMapFileCreator <input-path> <output-file-path>");
			System.exit(2);
		}
		final String DATA_FILE_NAME = "part";
		//first we sort sequence file
		Job job = Job.getInstance(conf, "Sequence File Sorter");
		job.setJarByClass(SequenceFileSorterMapFileCreator.class);
		job.setMapperClass(ReadFileMapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);		
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		
		job.waitForCompletion(true); 
		//then we rename the generated sorted sequence file from part-r-00000 to data to align it to MapFile
		final String dataFileNameWithPath = args[1] + File.separator + DATA_FILE_NAME + "-r-00000";
		FileSystem fs = FileSystem.get(URI.create(dataFileNameWithPath), conf);
		
		Path outputPath = new Path(args[1]); 
		Path dataFilePath = new Path(args[1] + File.separator + "data");
		fs.rename(new Path(dataFileNameWithPath),dataFilePath);
		// Get key and value types from data sequence file
		//you can use sorter method here to sort the file
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, dataFilePath, conf);
		Class keyClass = reader.getKeyClass();
		Class valueClass = reader.getValueClass();
		reader.close();
		
		// Create the map file index file
		long indexCount = MapFile.fix(fs, outputPath, keyClass, valueClass, false, conf);
		System.out.print("MapFile " + dataFilePath + "with " +  indexCount + " indexCount");
	}
}
