package com.cyl.hadoop.fiftyone.temperature;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexStudy {
	private static final Pattern qualityPattern = Pattern.compile("[01459]");

	public static void main(String[] args) {
		Matcher matcher = qualityPattern.matcher("14");
		boolean matches = matcher.matches();
		System.out.println(matches);
	}

}
