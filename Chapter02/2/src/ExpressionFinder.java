import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * 
 * 
 * @author hrishikesh
 *
 */
public class ExpressionFinder {

	public static class IdentifyTokenMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);


		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String param = conf.get("expression");
			
			for (String string : value.toString().split(param)) {
	            System.out.println(string);
	            context.write(new Text(string), one);
	        }
		}
	}

	public static class MatchedTermReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		if (args.length < 3) {
			System.err.println("Usage: ExpressionFinder <expression> <in> <out>");
			System.exit(2);
		}
		String expression = args[0];
		conf.set("expression",expression);
		Job job = Job.getInstance(conf, "expression finder");
		job.setJarByClass(ExpressionFinder.class);
		job.setMapperClass(IdentifyTokenMapper.class);
		job.setCombinerClass(MatchedTermReducer.class);
		job.setReducerClass(MatchedTermReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
