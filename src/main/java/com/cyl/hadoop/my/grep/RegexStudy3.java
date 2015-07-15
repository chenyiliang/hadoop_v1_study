package com.cyl.hadoop.my.grep;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 学习Java Regex用法，例子来自于Thinking in Java
 * 
 * 学习Group
 * 
 */
public class RegexStudy3 {
	private static final String POEM = "Twas brilling, and the slithy toves\n"
			+ "Did gyre and gimble in the wabe.\n"
			+ "All mimsy were the borogoves,"
			+ "And the mome raths outgrabe.\n\n"
			+ "Beware the Jabberwork, my son,\n"
			+ "The jaws that bite, the claws that catch.\n"
			+ "Beware the Jubjub bird, and shun\n"
			+ "The frumious Bandersnatch.";

	public static void main(String[] args) {
		Matcher m = Pattern.compile("(?m)(\\S+)\\s+((\\S+)\\s+(\\S+))$")
				.matcher(POEM);
		while (m.find()) {
			for (int i = 0; i <= m.groupCount(); i++) {
				System.out.print("[" + m.group(i) + "]");
			}
			System.out.println();
		}
	}

}
