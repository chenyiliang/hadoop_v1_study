package com.cyl.hadoop.fiftyone.temperature;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NewMaxTemperatureMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private static final int MISSING = 9999;
	private static final Pattern qualityPattern = Pattern.compile("[01459]");

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		System.out.println("key: " + key);
		String year = line.substring(15, 19);
		int airTemperature;
		if (line.charAt(45) == '+') {
			airTemperature = Integer.parseInt(line.substring(46, 50));
		} else {
			airTemperature = Integer.parseInt(line.substring(45, 50));
		}
		String quality = line.substring(50, 51);
		System.out.println("quality: " + quality);
		if (airTemperature != MISSING
				&& qualityPattern.matcher(quality).matches()) {
			context.write(new Text(year), new IntWritable(airTemperature));
		}
	}

}
