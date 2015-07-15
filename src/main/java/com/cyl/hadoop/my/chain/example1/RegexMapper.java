package com.cyl.hadoop.my.chain.example1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class RegexMapper extends
		Mapper<LongWritable, Conn2, LongWritable, Conn3> {
	@Override
	protected void map(LongWritable key, Conn2 value,
			Mapper<LongWritable, Conn2, LongWritable, Conn3>.Context context)
			throws IOException, InterruptedException {
		value.state = value.state.replaceAll("F", "Find");
		Conn3 c2 = new Conn3();
		c2.customer = value.customer;
		c2.orderDate = value.orderDate;
		c2.orderKey = value.orderKey;
		c2.price = value.price;
		c2.state = value.state;
		context.write(key, c2);
	}
}
