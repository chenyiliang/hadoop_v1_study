package com.cyl.hadoop.fiftyone.multiouts;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.cyl.hadoop.my.selfjoin.SelfJoinJob;

public class MultipleJob {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MultipleJob <in> <out>");
			System.exit(2);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(SelfJoinJob.class);

		job.setMapperClass(MultipleMapper.class);
		job.setReducerClass(MultipleReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		MultipleOutputs.addNamedOutput(job, "KeySplit", TextOutputFormat.class,
				NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "AllData", TextOutputFormat.class,
				NullWritable.class, Text.class);

		Path output = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
			System.out.println("输出路径存在，已删除！");
		}

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
