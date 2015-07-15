package com.cyl.hadoop.my.jobcontrol;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

import com.cyl.hadoop.my.grep.GrepJob;
import com.cyl.hadoop.my.grep.RegexMapper;

public class GrepJobController {
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Grep <inDir> <outDir> <regex> [<group>]");
			System.exit(-1);
		}

		Path tempDir = new Path("/data/grep-temp-"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		Configuration conf = new Configuration();
		conf.set("mapred.mapper.regex", args[2]);
		if (args.length == 4) {
			conf.set("mapred.mapper.regex.group", args[3]);
		}

		// grepJob
		Job grepJob = new Job(conf, "grep-search");
		grepJob.setJarByClass(GrepJob.class);
		grepJob.setMapperClass(RegexMapper.class);
		grepJob.setCombinerClass(LongSumReducer.class);
		grepJob.setReducerClass(LongSumReducer.class);
		grepJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		grepJob.setOutputKeyClass(Text.class);
		grepJob.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(grepJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(grepJob, tempDir);

		// sortJob
		Job sortJob = new Job(conf, "grep-sort");
		sortJob.setJarByClass(GrepJob.class);
		sortJob.setInputFormatClass(SequenceFileInputFormat.class);
		sortJob.setMapperClass(InverseMapper.class);
		sortJob.setNumReduceTasks(1);
		sortJob.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		FileInputFormat.addInputPath(sortJob, tempDir);
		FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));

		// ctrlGrepJob
		ControlledJob ctrlGrepJob = new ControlledJob(conf);
		ctrlGrepJob.setJob(grepJob);

		// ctrlSortJob
		ControlledJob ctrlSortJob = new ControlledJob(conf);
		ctrlSortJob.setJob(sortJob);
		ctrlSortJob.addDependingJob(ctrlGrepJob);

		// JobControl
		JobControl jobControl = new JobControl("grep-sort");
		jobControl.addJob(ctrlGrepJob);
		jobControl.addJob(ctrlSortJob);

		// 要注意的地方就是hadoop的JobControl类实现了线程Runnable接口。
		// 我们需要实例化一个线程来让它启动。直接调用JobControl的run()方法，线程将无法结束。
		Thread JCThread = new Thread(jobControl);
		JCThread.start();
		while (true) {
			if (jobControl.allFinished()) {
				System.out.println(jobControl.getSuccessfulJobList());
				jobControl.stop();
				System.exit(0);
			}
			if (jobControl.getFailedJobList().size() > 0) {
				System.out.println(jobControl.getFailedJobList());
				jobControl.stop();
				System.exit(1);
			}
		}
	}
}
