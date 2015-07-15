package com.cyl.hadoop.fiftyone.deduplication;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	private Text newKey = new Text();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString().trim();
		if (line.length() > 0) {
			newKey.set(line);
			context.write(newKey, NullWritable.get());
		}
	}
}
