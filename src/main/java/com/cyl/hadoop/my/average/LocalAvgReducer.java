package com.cyl.hadoop.my.average;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LocalAvgReducer extends Reducer<Text, Text, Text, Text> {
	private Pattern longPattern = Pattern.compile("\\d+");
	private Text newValue = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		if (key.toString().trim().length() < 1) {
			return;
		}

		long sum = 0;
		long keyCounter = 0;
		for (Text val : values) {
			String valStr = val.toString().trim();
			if (valStr.length() > 0 && longPattern.matcher(valStr).matches()) {
				sum += Long.parseLong(valStr);
				keyCounter++;
			}
		}
		newValue.set(sum + ":" + keyCounter);
		context.write(key, newValue);
		System.out.println(key.toString() + ":" + newValue.toString());
	}
}
