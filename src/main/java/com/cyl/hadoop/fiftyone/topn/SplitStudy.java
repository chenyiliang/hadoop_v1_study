package com.cyl.hadoop.fiftyone.topn;

import java.util.Arrays;

public class SplitStudy {

	public static void main(String[] args) {
		String str = "1:a:b:c:";
		String[] split = str.split(":", -1);
		System.out.println(Arrays.toString(split));

		int[] arr = { 4, 8, 12, 4, 6 };
		Arrays.sort(arr);
		System.out.println(Arrays.toString(arr));
	}

}
