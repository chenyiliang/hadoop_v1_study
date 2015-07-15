package com.cyl.hadoop.guide.counter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class CounterMapper extends
		Mapper<LongWritable, Text, NullWritable, Text> {
	private Counter counter;

	@Override
	protected void setup(
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		counter = context.getCounter("job-log", "line-count");
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		counter.increment(1);
	}
}
