package com.cyl.hadoop.guide.tera.gen;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RangeRecordReader extends RecordReader<LongWritable, NullWritable> {
	long startRow;
	long finishedRows;
	long totalRows;
	LongWritable key = null;

	public RangeRecordReader() {
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		startRow = ((RangeInputSplit) split).firstRow;
		finishedRows = 0;
		totalRows = ((RangeInputSplit) split).rowCount;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key == null) {
			key = new LongWritable();
		}
		if (finishedRows < totalRows) {
			key.set(startRow + finishedRows);
			finishedRows += 1;
			return true;
		} else {
			return false;
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public NullWritable getCurrentValue() throws IOException,
			InterruptedException {
		return NullWritable.get();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return finishedRows / (float) totalRows;
		// ? 为什么源代码这要强制转换成float
	}

	@Override
	public void close() throws IOException {
	}

}
