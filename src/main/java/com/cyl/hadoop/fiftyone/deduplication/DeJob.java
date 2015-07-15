package com.cyl.hadoop.fiftyone.deduplication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DeJob {

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.print("Usage: DeJob <input path...> <output path>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf, "de-job");
		job.setJarByClass(DeJob.class);

		job.setMapperClass(DeMapper.class);
		job.setReducerClass(DeReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		for (int i = 0; i < args.length - 1; i++) {
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}

		Path outPath = new Path(args[args.length - 1]);
		FileOutputFormat.setOutputPath(job, outPath);

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
