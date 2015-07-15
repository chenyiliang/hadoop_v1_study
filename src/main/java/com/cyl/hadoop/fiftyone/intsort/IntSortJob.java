package com.cyl.hadoop.fiftyone.intsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IntSortJob {

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.print("Usage: IntSortJob <input path...> <output path>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf, "int-sort");
		job.setJarByClass(IntSortJob.class);

		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);

		job.setNumReduceTasks(3);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

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
