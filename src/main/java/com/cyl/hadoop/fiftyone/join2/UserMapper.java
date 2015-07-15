package com.cyl.hadoop.fiftyone.join2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserMapper extends Mapper<LongWritable, Text, LongWritable, User> {
	private LongWritable newKey = new LongWritable();
	private User user = new User();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, LongWritable, User>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] arr = line.split("\t");
		if (arr.length == 2) {// city
			user.setCityNo(arr[0]);
			user.setCityName(arr[1]);
			user.setFlag(1);
			newKey.set(Long.valueOf(user.getCityNo()));
			context.write(newKey, user);
		} else {// user
			user.setUserNo(arr[0]);
			user.setUserName(arr[1]);
			user.setCityNo(arr[2]);
			user.setFlag(0);
			newKey.set(Long.valueOf(user.getCityNo()));
			context.write(newKey, user);
		}
	}
}
