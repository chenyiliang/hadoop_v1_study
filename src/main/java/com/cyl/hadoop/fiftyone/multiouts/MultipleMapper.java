package com.cyl.hadoop.fiftyone.multiouts;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultipleMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	private IntWritable newKey = new IntWritable();
	private Text newValue = new Text();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString().trim();
		String[] arr = line.split(",");
		if (arr.length > 0) {
			newKey.set(Integer.parseInt(arr[0]));
			newValue.set(line);
			context.write(newKey, newValue);
		}
	}
}
