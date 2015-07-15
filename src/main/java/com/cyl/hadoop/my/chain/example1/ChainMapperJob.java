package com.cyl.hadoop.my.chain.example1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapreduce.Job;

public class ChainMapperJob {
	public static void main(String[] args) throws Exception {
		JobConf job = new JobConf(ChainMapperJob.class);
		job.setNumReduceTasks(0);
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		ChainMapper cm = new ChainMapper();
	}
}
