package com.cyl.hadoop.my.grep;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RegexMapper<K> extends Mapper<K, Text, Text, LongWritable> {
	private Pattern pattern;
	private int group;

	private Text word = new Text();
	private static final LongWritable one = new LongWritable(1);

	@Override
	protected void setup(Mapper<K, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		pattern = Pattern.compile(conf.get("mapred.mapper.regex"));
		group = conf.getInt("mapred.mapper.regex.group", 0);
	}

	@Override
	protected void map(K key, Text value,
			Mapper<K, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		Matcher matcher = pattern.matcher(line);
		while (matcher.find()) {
			word.set(matcher.group(group));
			context.write(word, one);
		}
	}

}
