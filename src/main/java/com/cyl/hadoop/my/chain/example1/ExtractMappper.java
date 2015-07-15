package com.cyl.hadoop.my.chain.example1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ExtractMappper extends
		Mapper<LongWritable, Text, LongWritable, Conn1> {
	private LongWritable lw = new LongWritable();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, LongWritable, Conn1>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] strs = line.split(";");
		Conn1 conn1 = new Conn1();
		conn1.orderKey = Long.parseLong(strs[0]);
		conn1.customer = Long.parseLong(strs[1]);
		conn1.state = strs[2];
		conn1.price = Double.parseDouble(strs[3]);
		conn1.orderDate = DateUtil.getDateFromString(strs[4], "yyyy-MM-dd");
		lw.set(conn1.orderKey);
		context.write(lw, conn1);
	}
}
