package com.cyl.hadoop.my.grep;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexStudy2 {

	/**
	 * 学习Java Regex用法，例子来自于Thinking in Java
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Matcher m = Pattern.compile("\\w+").matcher(
				"Evening is full of the linner's wing");
		while (m.find()) {
			System.out.print(m.group() + ":");
		}
		System.out.println("\r\n-------------------------------------");
		int i = 0;//偏移量
		while (m.find(i)) {
			System.out.print(m.group() + ":");
			i++;
		}
	}

}
