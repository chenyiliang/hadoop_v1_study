package com.cyl.hadoop.fiftyone.join3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.cyl.hadoop.fiftyone.join2.User;

public class UserReducer extends Reducer<UserKey, User, LongWritable, Text> {
	@Override
	protected void reduce(UserKey key, Iterable<User> values,
			Reducer<UserKey, User, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		User city = new User();
		int num = 0;
		for (User u : values) {// 第一个数据是city
			if (num == 0) {
				city = new User(u);
				System.out.println(city);
			} else {
				User tmp = new User(u);
				tmp.setCityName(city.getCityName());
				context.write(new LongWritable(Long.valueOf(tmp.getUserNo())),
						new Text(tmp.toString()));
			}
			num++;
		}
	}
}
