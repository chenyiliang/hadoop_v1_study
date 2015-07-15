package com.cyl.hadoop.my.chain.example1;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public abstract class DateUtil {
	public static Date getDateFromString(String source, String pattern) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
		Date date;
		try {
			date = simpleDateFormat.parse(source);
			return date;
		} catch (ParseException e) {
			throw new IllegalArgumentException(
					"The date string source pattern is not correct!", e);
		}

	}

	public static String getDateStr(Date date, String pattern) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
		String format = simpleDateFormat.format(date);
		return format;

	}
}
