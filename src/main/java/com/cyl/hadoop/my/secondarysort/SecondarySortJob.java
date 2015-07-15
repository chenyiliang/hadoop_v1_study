package com.cyl.hadoop.my.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondarySortJob {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: secondarysort <in> <out>");
			System.exit(2);
		}

		Configuration conf = new Configuration();

		Job job = new Job(conf, "secondary sort");
		job.setJarByClass(SecondarySortJob.class);
		job.setMapperClass(SndSortMapper.class);
		job.setReducerClass(SndSortReducer.class);

		// group and partition by the first int in the pair
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);

		// the map output is IntPair, IntWritable
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		// the reduce output is Text, IntWritable
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
