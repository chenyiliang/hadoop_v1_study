package com.cyl.hadoop.my.chain.example1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Filter1Mapper extends
		Mapper<LongWritable, Conn1, LongWritable, Conn2> {
	@Override
	protected void map(LongWritable key, Conn1 value,
			Mapper<LongWritable, Conn1, LongWritable, Conn2>.Context context)
			throws IOException, InterruptedException {
		if (value.state.equals("F")) {
			Conn2 inValue = new Conn2();
			inValue.customer = value.customer;
			inValue.orderDate = value.orderDate;
			inValue.orderKey = value.orderKey;
			inValue.price = value.price;
			inValue.state = value.state;
			context.write(key, inValue);
		}
	}
}
