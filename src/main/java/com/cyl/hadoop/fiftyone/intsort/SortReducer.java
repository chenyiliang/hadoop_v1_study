package com.cyl.hadoop.fiftyone.intsort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends
		Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {
	private IntWritable rank = new IntWritable(1);

	@Override
	protected void reduce(
			IntWritable key,
			Iterable<NullWritable> values,
			Reducer<IntWritable, NullWritable, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		for (NullWritable nw : values) {
			context.write(rank, key);
			rank.set(rank.get() + 1);
		}
	}
}
