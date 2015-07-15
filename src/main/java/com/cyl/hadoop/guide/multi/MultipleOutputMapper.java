package com.cyl.hadoop.guide.multi;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultipleOutputMapper extends
		Mapper<LongWritable, Text, NullWritable, Text> {
	private NullWritable nw = NullWritable.get();
	private Text word = new Text();

	private MultipleOutputs<NullWritable, Text> mos;

	@Override
	protected void setup(
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		mos = new MultipleOutputs<NullWritable, Text>(context);
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		StringTokenizer words = new StringTokenizer(value.toString());
		while (words.hasMoreTokens()) {
			String token = words.nextToken();
			word.set(token);

			mos.write(nw, word, token.charAt(0) + "/");
		}
	}

	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		mos.close();
	}

}
