package com.cyl.hadoop.my.chain.example1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class LoadMapper extends
		Mapper<LongWritable, Conn3, LongWritable, Conn3> {
	@Override
	protected void map(LongWritable key, Conn3 value,
			Mapper<LongWritable, Conn3, LongWritable, Conn3>.Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}
