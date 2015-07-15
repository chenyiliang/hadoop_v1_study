package com.cyl.hadoop.guide.tera.gen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RangeInputFormat extends InputFormat<LongWritable, NullWritable> {

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException,
			InterruptedException {
		// 数据总共有几行
		long totalRows = getNumberOfRows(job);
		// 数据要分成几块对应输入到几个mapper
		int numSplits = job.getConfiguration().getInt("mapreduce.job.maps", 1);
		List<InputSplit> splits = new ArrayList<InputSplit>();
		long currentRow = 0;
		for (int split = 0; split < numSplits; ++split) {
			long goal = (long) Math.ceil(totalRows * (double) (split + 1)/ numSplits);
			splits.add(new RangeInputSplit(currentRow, goal - currentRow));
			currentRow = goal;
		}
		return splits;
	}

	@Override
	public RecordReader<LongWritable, NullWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new RangeRecordReader();
	}

	static long getNumberOfRows(JobContext job) {
		return job.getConfiguration().getLong("terasort.num-rows", 0);
	}

	static void setNumberOfRows(Job job, long numRows) {
		job.getConfiguration().setLong("terasort.num-rows", numRows);
	}

}
