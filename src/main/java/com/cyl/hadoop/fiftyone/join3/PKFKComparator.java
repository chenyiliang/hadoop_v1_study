package com.cyl.hadoop.fiftyone.join3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PKFKComparator extends WritableComparator {

	// http://www.linuxidc.com/Linux/2013-09/89652.htm
	protected PKFKComparator() {
		super(UserKey.class, true);// 必须为true
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		UserKey a1 = (UserKey) a;
		UserKey b1 = (UserKey) b;
		System.out.println(" call pkfk comparator");
		if (a1.getKeyid() == b1.getKeyid()) {
			return 0;
		} else {
			return a1.getKeyid() > b1.getKeyid() ? 1 : -1;
		}
	}

}
