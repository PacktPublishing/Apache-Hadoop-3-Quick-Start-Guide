package org.hk.book.hadoop3.examples;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;



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
 * This is a sequence file reader class
 * @author hrishikesh.k
 *
 */

public class SequenceFileReader {
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

	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		if (args.length < 2) {
			System.err.println("Usage: SequenceFileReader <input-seq-file> <output-text-file-path>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Sequence File Creator");
		job.setJarByClass(SequenceFileReader.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(ReadFileMapper.class);
		

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
