package com.cyl.hadoop.fiftyone.topn;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper extends
		Mapper<LongWritable, Text, IntWritable, NullWritable> {
	private int[] topN;
	private Pattern intPattern = Pattern.compile("\\d+");
	private IntWritable topKey = new IntWritable();

	@Override
	protected void setup(
			Mapper<LongWritable, Text, IntWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		int len = context.getConfiguration().getInt("N", 10);
		topN = new int[len + 1];
	}

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, IntWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String[] rawArray = value.toString().trim().split(",");
		for (int i = 0; i < rawArray.length; i++) {
			String rawStr = rawArray[i].trim();
			if (rawStr.length() > 0 && intPattern.matcher(rawStr).matches()) {
				int n = Integer.parseInt(rawStr);
				topN[0] = n;
				Arrays.sort(topN);
			}

		}
		super.map(key, value, context);
	}

	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, IntWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		for (int i = 1; i < topN.length; i++) {
			topKey.set(topN[i]);
			context.write(topKey, NullWritable.get());
		}
	}
}
