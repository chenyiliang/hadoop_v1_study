package com.cyl.hadoop.my.grep;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexStudy1b {

	/**
	 * 获取“中括号[]”中的字段
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Matcher m1 = Pattern.compile("\\[([^\\[\\]]+)\\]").matcher(
				"[name][age][address]");
		while (m1.find()) {
			System.out.print(m1.group(1) + ":");
		}
		System.out.println("\r\n---------------------------");
		Matcher m2 = Pattern.compile("b|a|c").matcher("abc");
		while (m2.find()) {
			System.out.print(m2.group(0) + ":");
		}
	}

}
