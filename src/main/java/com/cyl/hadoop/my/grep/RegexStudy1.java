package com.cyl.hadoop.my.grep;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexStudy1 {

	/**
	 * 学习Java Regex用法，例子来自于Thinking in Java
	 * 
	 * 输入参数查看结果:abcabcabcdefabc "abc+" "(abc)+" "(abc){2,}"
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage:");
			System.exit(0);
		}
		System.out.println("Input:\"" + args[0] + "\"");
		for (String arg : args) {
			System.out.println("Regular expression:\"" + arg + "\"");
			Pattern p = Pattern.compile(arg);
			Matcher m = p.matcher(args[0]);
			while (m.find()) {
				System.out.println("Match \"" + m.group() + "\" at position "
						+ m.start() + "-" + (m.end() - 1));
			}
		}
	}

}
