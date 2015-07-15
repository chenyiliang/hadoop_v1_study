package com.cyl.hadoop.fiftyone.deduplication;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DeReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,
			Reducer<Text, NullWritable, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}
}
