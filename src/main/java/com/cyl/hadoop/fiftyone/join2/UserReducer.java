package com.cyl.hadoop.fiftyone.join2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserReducer extends
		Reducer<LongWritable, User, LongWritable, Text> {
	private LongWritable outKey = new LongWritable();
	private Text outValue = new Text();

	@Override
	protected void reduce(LongWritable key, Iterable<User> values,
			Reducer<LongWritable, User, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		User city = null;
		List<User> list = new ArrayList<User>();
		for (User u : values) {
			if (u.getFlag() == 0) {
				User tmp = new User(u);
				list.add(tmp);
			} else {
				city = new User(u);

			}
		}

		for (User tmp : list) {
			tmp.setCityName(city.getCityName());
			outKey.set(Long.parseLong(tmp.getUserNo()));
			outValue.set(tmp.toString());
			context.write(outKey, outValue);
		}
	}
}
