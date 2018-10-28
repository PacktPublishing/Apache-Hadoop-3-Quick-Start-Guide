package org.hk.book.hadoop3.examples;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;


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
* This is an example of Multiple Output File Format processing.
* 
* @author hrishikesh.k
*
*/

public class SuperStoreAnalyzer {

	public static class SuperStoreMapper extends Mapper<LongWritable, Text, Text, Text> {
		  
		 private Text txtKey = new Text("");
		 private Text txtVal = new Text("");
		 
		 
		 @Override
		 protected void map(LongWritable key, Text value,Context context)
		   throws IOException, InterruptedException {
		  if(value.toString().length() > 0) {
		   String[] custArray = value.toString().split(",");
		   //get the region information
		   if (custArray.length > 15) {
			   txtKey.set(custArray[14].toString());
			   txtVal.set(custArray[5].toString());
			   context.write(txtKey, txtVal);
		   }
		   else if(custArray.length > 6) {
			   txtKey.set("Regionless");
			   txtVal.set(custArray[5].toString());
			   context.write(txtKey, txtVal);
		   }
		   
		   
		  }
		 }	 
	}
	
	
	public static class SuperStoreReducer extends Reducer<Text, Text, Text, Text>{
		  
		 private MultipleOutputs<Text, Text> multipleOutputs;
		  
		 @Override
		 protected void setup(Context context) throws IOException, InterruptedException {
		  multipleOutputs  = new MultipleOutputs<Text, Text>(context);
		 }
		  
		 @Override
		 protected void reduce(Text key, Iterable<Text> values,Context context)
		   throws IOException, InterruptedException {
		  for(Text value : values) {
		   multipleOutputs.write(key, value, key.toString());
		  }
		 }
		  
		 @Override
		 protected void cleanup(Context context)
		   throws IOException, InterruptedException {
		  multipleOutputs.close();
		 }
		 
		}
	
	
	public static void main(String[] args) 
			throws Exception {
		Configuration conf = new Configuration();
		
		if (args.length < 2) {
			System.err.println("Usage: SuperStoreAnalyzer <input-file> <output-file-path>");
			System.exit(2);
		}

		//first we sort sequence file
		Job job = Job.getInstance(conf, "Super Store Analyzer");
		job.setJarByClass(SuperStoreAnalyzer.class);
		job.setMapperClass(SuperStoreMapper.class);		
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//FileInputFormat.setInputPaths(job, new Path("D:\\hrishi\\Personal\\Hadoop3-book\\source\\git\\Chapter4\\input"));
		//FileOutputFormat.setOutputPath(job, new Path("D:\\hrishi\\Personal\\Hadoop3-book\\source\\git\\Chapter4\\output\1"));		
		
		job.setMapperClass(SuperStoreMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setReducerClass(SuperStoreReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
	
		job.waitForCompletion(true);
	}
}
