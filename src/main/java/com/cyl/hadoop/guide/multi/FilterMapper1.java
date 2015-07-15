package com.cyl.hadoop.guide.multi;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper1 extends Mapper<Text, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outValue = new Text();

	@Override
	protected void map(Text key, Text value,
			Mapper<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		outKey.set("[" + key.toString() + "]");
		outValue.set("[" + value.toString() + "]");
		context.write(outKey, outValue);
	}
}
