package com.cyl.hadoop.fiftyone.join3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.cyl.hadoop.fiftyone.join2.User;

public class UserMapper extends Mapper<LongWritable, Text, UserKey, User> {
	private UserKey userKey = new UserKey();
	private User user = new User();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, UserKey, User>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString().trim();
		String[] arr = line.split("\t");
		if (arr.length == 2) {// city
			user.setCityNo(arr[0]);
			user.setCityName(arr[1]);
			user.setFlag(1);

			userKey.setKeyid(Integer.parseInt(arr[0]));
			userKey.setPrimary(false);

		} else {// user
			user.setUserNo(arr[0]);
			user.setUserName(arr[1]);
			user.setCityNo(arr[2]);
			user.setFlag(0);

			userKey.setKeyid(Integer.valueOf(arr[2]));
			userKey.setPrimary(true);
		}
		System.out.println(userKey);
		context.write(userKey, user);
	}
}
