package com.cyl.hadoop.guide.multi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MultipleOutputJob {

	// 此用例相对简单，未显示MultipleOutputs全部用法，详见http://tydldd.iteye.com/blog/2053867
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: <in> <out>");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		Job job = new Job(conf, "multi-out");
		job.setJarByClass(MultipleOutputJob.class);
		job.setNumReduceTasks(0);
		job.setMapperClass(MultipleOutputMapper.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
