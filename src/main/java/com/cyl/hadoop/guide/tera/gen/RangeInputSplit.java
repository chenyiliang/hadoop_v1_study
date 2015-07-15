package com.cyl.hadoop.guide.tera.gen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;

public class RangeInputSplit extends InputSplit implements Writable {
	long firstRow;
	long rowCount;

	public RangeInputSplit() {
	}

	public RangeInputSplit(long offset, long length) {
		firstRow = offset;
		rowCount = length;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVLong(out, firstRow);
		WritableUtils.writeVLong(out, rowCount);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		firstRow = WritableUtils.readVLong(in);
		rowCount = WritableUtils.readVLong(in);
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[] {};
	}

}
