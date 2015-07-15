package com.cyl.hadoop.my.grep;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//http://blog.pureisle.net/archives/1687.html
public class GrepJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Path tempDir = new Path("/data/grep-temp-"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		Job grepJob = new Job(getConf(), "grep-search");

		try {

			grepJob.setJarByClass(GrepJob.class);
			grepJob.setMapperClass(RegexMapper.class);
			grepJob.setCombinerClass(LongSumReducer.class);
			grepJob.setReducerClass(LongSumReducer.class);
			grepJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			grepJob.setOutputKeyClass(Text.class);
			grepJob.setOutputValueClass(LongWritable.class);
			FileInputFormat.addInputPath(grepJob, new Path(args[0]));
			FileOutputFormat.setOutputPath(grepJob, tempDir);
			boolean grepSuccess = grepJob.waitForCompletion(true);

			System.out.println("grepSuccess:" + grepSuccess);
			if (!grepSuccess) {
				System.exit(1);
			}

			Job sortJob = new Job(getConf(), "grep-sort");
			sortJob.setJarByClass(GrepJob.class);
			sortJob.setInputFormatClass(SequenceFileInputFormat.class);
			sortJob.setMapperClass(InverseMapper.class);
			//sortJob.setNumReduceTasks(1);
			sortJob.setSortComparatorClass(LongWritable.DecreasingComparator.class);
			FileInputFormat.addInputPath(sortJob, tempDir);
			FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));
			boolean sortSuccess = sortJob.waitForCompletion(true);

			return sortSuccess ? 0 : 1;
		} finally {
			// FileSystem.get(getConf()).delete(tempDir, true);
		}

	}

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("Grep <inDir> <outDir> <regex> [<group>]");
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		conf.set("mapred.mapper.regex", args[2]);
		if (args.length == 4) {
			conf.set("mapred.mapper.regex.group", args[3]);
		}

		int res = ToolRunner.run(conf, new GrepJob(), args);
		System.exit(res);
	}

}
