package com.cyl.hadoop.fiftyone.multiouts;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultipleReducer extends
		Reducer<IntWritable, Text, NullWritable, Text> {
	private MultipleOutputs<NullWritable, Text> mos = null;

	@Override
	protected void setup(
			Reducer<IntWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		mos = new MultipleOutputs<NullWritable, Text>(context);
	}

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Reducer<IntWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		for (Text val : values) {
			mos.write("KeySplit", NullWritable.get(), val, key.toString() + "/");
			mos.write("AllData", NullWritable.get(), val);
		}
	}

	@Override
	protected void cleanup(
			Reducer<IntWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		mos.close();
	}
}
