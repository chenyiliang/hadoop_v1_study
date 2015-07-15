package com.cyl.hadoop.guide.keyvalue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KVReadFormatJob {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: <in> <out>");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		conf.set(
				"mapreduce.input.keyvaluelinerecordreader.key.value.separator",
				":");// 用→会有乱码
		Job job = new Job(conf);
		job.setJarByClass(KVReadFormatJob.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
