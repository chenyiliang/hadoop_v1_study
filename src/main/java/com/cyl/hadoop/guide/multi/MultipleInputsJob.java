package com.cyl.hadoop.guide.multi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MultipleInputsJob {

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: <in1> <in2> <out>");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		conf.set(
				"mapreduce.input.keyvaluelinerecordreader.key.value.separator",
				":");//
		Job job = new Job(conf, "multi1");
		job.setJarByClass(MultipleInputsJob.class);
		// job.setNumReduceTasks(0);
		// reducer为0，两个mapper会有两个文件
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// reducer为0，就无需以上3行
		MultipleInputs.addInputPath(job, new Path(args[0]),
				KeyValueTextInputFormat.class, FilterMapper1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				KeyValueTextInputFormat.class, FilterMapper2.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
