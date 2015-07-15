package com.cyl.hadoop.my.average;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * key1 100
 * key1 200
 * key1 350
 * 
 * key2 348
 * key2 456
 * key2 786
 * 
 * avg(key1),avg(key2),...
 */
public class SumMapper extends Mapper<Text, LongWritable, Text, Text> {
	@Override
	protected void map(Text key, LongWritable value,
			Mapper<Text, LongWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
	}
}
