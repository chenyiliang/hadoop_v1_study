package com.cyl.hadoop.fiftyone.intsort;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends
		Mapper<LongWritable, Text, IntWritable, NullWritable> {
	private IntWritable intKey = new IntWritable();
	private Pattern intPattern = Pattern.compile("\\d+");

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, IntWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString().trim();
		if (line.length() > 0 && intPattern.matcher(line).matches()) {
			intKey.set(Integer.parseInt(line));
			context.write(intKey, NullWritable.get());
		}
	}
}
