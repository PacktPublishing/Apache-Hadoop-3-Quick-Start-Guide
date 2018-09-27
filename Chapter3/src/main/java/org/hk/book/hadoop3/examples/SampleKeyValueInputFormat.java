package org.hk.book.hadoop3.examples;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class SampleKeyValueInputFormat extends KeyValueInputFormat {
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}
}