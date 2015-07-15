package com.cyl.hadoop.my.average;

import java.util.regex.Pattern;

public class PatternTest {

	public static void main(String[] args) {
		Pattern pattern = Pattern.compile("\\d+:\\d+");
		boolean matches = pattern.matcher("12:3").matches();
		System.out.println(matches);

		double d = (double) 1 / 9;
		System.out.println(d);
	}

}
