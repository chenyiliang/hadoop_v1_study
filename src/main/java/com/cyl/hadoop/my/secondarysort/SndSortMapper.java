package com.cyl.hadoop.my.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SndSortMapper extends
		Mapper<LongWritable, Text, IntPair, IntWritable> {
	private IntPair newKey = new IntPair();
	private IntWritable newValue = new IntWritable();

	// private Pattern inputPattern = Pattern.compile("\\d+\\t\\d+");

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntPair, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString().trim();
		String[] arr = line.split("\t");
		int first = Integer.parseInt(arr[0]);
		int second = Integer.parseInt(arr[1]);

		newKey.setFirst(first);
		newKey.setSecond(second);
		newValue.set(second);

		context.write(newKey, newValue);
	}
}
