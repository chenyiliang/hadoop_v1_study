package com.cyl.hadoop.my.average;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgJob {

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.print("Usage: AvgJob <input path...> <output path>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		conf.set(
				"mapreduce.input.keyvaluelinerecordreader.key.value.separator",
				":");// 用→会有乱码
		Job job = new Job(conf, "avg");
		job.setJarByClass(AvgJob.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setCombinerClass(LocalAvgReducer.class);
		job.setReducerClass(CentralAvgReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

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
