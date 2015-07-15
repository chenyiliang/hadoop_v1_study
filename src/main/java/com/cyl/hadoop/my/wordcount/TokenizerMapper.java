package com.cyl.hadoop.my.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private static final IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		StringTokenizer words = new StringTokenizer(value.toString());
		while (words.hasMoreTokens()) {
			word.set(words.nextToken());
			context.write(word, one);
		}
	}

}
