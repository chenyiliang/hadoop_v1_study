package com.cyl.hadoop.my.chain.example1;

import java.text.ParseException;
import java.util.Date;

public class Test {

	public static void main(String[] args) throws ParseException {
		Date date = DateUtil.getDateFromString("1991-10-07", "yyyy-MM-dd");
		System.out.println(date);
	}

}
