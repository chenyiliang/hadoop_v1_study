package com.cyl.hadoop.my.grep;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 学习Java Regex用法，例子来自于Thinking in Java
 * 
 * 学习Group
 * 
 */
public class RegexStudy3c {
	private static final String str = "Main:1991--[sssss]";

	public static void main(String[] args) {
		Matcher m = Pattern.compile("Main:(\\d+)--\\[(\\w+)\\]").matcher(str);
		while (m.find()) {
			for (int i = 0; i <= m.groupCount(); i++) {
				System.out.print("[" + m.group(i) + "]");
			}
			System.out.println();
		}
	}

}
