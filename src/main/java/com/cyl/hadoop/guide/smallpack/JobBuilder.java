package com.cyl.hadoop.guide.smallpack;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public abstract class JobBuilder {
	public static Job parseInputAndOutput(Tool tool, Configuration conf,
			String[] args) {
		if (args.length != 2) {
			printUsage(tool, "<input><output>");
			return null;
		}
		Job job;
		try {
			job = new Job(conf);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			return job;
		} catch (IOException e) {
			return null;
		}
	}

	public static void printUsage(Tool tool, String extraArgsUsage) {
		System.err.printf("Usage:%s [genericOptions] %s\n\n", tool.getClass()
				.getSimpleName(), extraArgsUsage);
	}
}
