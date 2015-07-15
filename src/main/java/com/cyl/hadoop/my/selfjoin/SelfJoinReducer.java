package com.cyl.hadoop.my.selfjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SelfJoinReducer extends Reducer<Text, Text, Text, Text> {
	private List<String> left = new ArrayList<String>();
	private List<String> right = new ArrayList<String>();

	private Text outKey = new Text();
	private Text outValue = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		for (Text text : values) {
			String value = text.toString();
			if (value.endsWith(":-")) {
				left.add(value);
			} else if (value.endsWith(":+")) {
				right.add(value);
			}
		}

		// 笛卡尔积
		if (left.size() > 0 && right.size() > 0) {
			for (String lval : left) {
				for (String rval : right) {
					outKey.set(lval.substring(0, lval.length() - 2));
					outValue.set(rval.substring(0, rval.length() - 2));
					context.write(outKey, outValue);
				}
			}
		}

		// 清空List
		left.clear();
		right.clear();
	}
}
