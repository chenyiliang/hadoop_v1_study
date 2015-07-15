package com.cyl.hadoop.my.average;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CentralAvgReducer extends Reducer<Text, Text, Text, Text> {
	private Pattern pattern = Pattern.compile("\\d+:\\d+");
	private Text avgResult = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		long sum = 0;
		long keyCounter = 0;
		for (Text val : values) {
			String valStr = val.toString().trim();
			if (valStr.length() > 2 && pattern.matcher(valStr).matches()) {
				String[] sumPair = valStr.split(":");
				sum += Long.parseLong(sumPair[0]);
				keyCounter += Long.parseLong(sumPair[1]);
			}
		}
		double avg = (double) sum / keyCounter;
		avgResult.set(String.valueOf(avg));
		context.write(key, avgResult);
	}
}
