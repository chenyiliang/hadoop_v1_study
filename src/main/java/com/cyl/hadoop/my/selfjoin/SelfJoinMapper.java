package com.cyl.hadoop.my.selfjoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SelfJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text newKey = new Text();
	private Text newValue = new Text();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString().trim();
		String[] arr = line.split("\t");
		if (arr.length == 2) {
			newKey.set(arr[0]);
			newValue.set(arr[1] + ":+");
			context.write(newKey, newValue);

			newKey.set(arr[1]);
			newValue.set(arr[0] + ":-");
			context.write(newKey, newValue);
		}
	}
}
