package com.cyl.hadoop.my.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntPair implements WritableComparable<IntPair> {
	private int first = 0;
	private int second = 0;

	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(first - Integer.MIN_VALUE);// 这么做是为了减少数据传送量？
		out.writeInt(second - Integer.MIN_VALUE);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readInt() + Integer.MIN_VALUE;
		second = in.readInt() + Integer.MIN_VALUE;
	}

	@Override
	public int compareTo(IntPair o) {
		if (first != o.first) {
			return first < o.first ? -1 : 1;
		} else if (second != o.second) {
			return second < o.second ? -1 : 1;
		} else {
			return 0;
		}
	}

	@Override
	public int hashCode() {
		return first * 157 + second;// 是为了提高差异率？
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof IntPair) {
			IntPair r = (IntPair) obj;
			return r.first == first && r.second == second;
		} else {
			return false;
		}
	}

}
