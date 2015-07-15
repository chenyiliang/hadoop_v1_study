package com.cyl.hadoop.guide.counter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CounterJob {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: <in> <out>");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		Job job = new Job(conf, "counter");
		job.setJarByClass(CounterJob.class);
		job.setMapperClass(CounterMapper.class);
		job.setNumReduceTasks(0);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
