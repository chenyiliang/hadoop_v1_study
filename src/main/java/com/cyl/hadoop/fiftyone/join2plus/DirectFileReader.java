package com.cyl.hadoop.fiftyone.join2plus;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DirectFileReader {
	private static final Path path = new Path("/data/in9/int.data");

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream rawIn = fs.open(path);
		InputStreamReader isr = new InputStreamReader(rawIn, "utf-8");
		BufferedReader bfr = new BufferedReader(isr);
		String line = null;
		while ((line = bfr.readLine()) != null) {
			System.out.println(line);
		}
	}

}
