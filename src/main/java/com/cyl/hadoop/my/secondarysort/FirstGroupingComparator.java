package com.cyl.hadoop.my.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FirstGroupingComparator extends WritableComparator {

	protected FirstGroupingComparator() {
		super(IntPair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		IntPair intPair1 = (IntPair) a;
		IntPair intPair2 = (IntPair) b;
		Integer f1 = intPair1.getFirst();
		Integer f2 = intPair2.getFirst();
		return f1.compareTo(f2);
	}

}
