package com.cyl.hadoop.my.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SndSortReducer extends
		Reducer<IntPair, IntWritable, Text, IntWritable> {
	private final Text first = new Text();

	@Override
	protected void reduce(IntPair key, Iterable<IntWritable> values,
			Reducer<IntPair, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		first.set(Integer.toString(key.getFirst()));
		for (IntWritable value : values) {
			context.write(first, value);
		}
	}
}
