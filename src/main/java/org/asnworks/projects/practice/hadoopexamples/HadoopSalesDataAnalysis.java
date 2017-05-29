package org.asnworks.projects.practice.hadoopexamples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HadoopSalesDataAnalysis {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "HadoopSalesDataAnalysis");

		job.setJarByClass(HadoopSalesDataAnalysis.class);

		// Total price per country
		// job.setMapperClass(SalesDataMapper.class);
		// job.setReducerClass(SalesDataReducer.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path outputPath = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		outputPath.getFileSystem(conf).delete(outputPath, true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class SalesDataMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			String parts[] = line.split(",");
			String price = parts[2];
			String coutry = parts[7];

			value.set(coutry);
			System.out.println(coutry + " " + price);
			context.write(value, new LongWritable(Long.parseLong(price)));
		}

	}

	public static class SalesDataReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {

			long sum = 0;

			for (LongWritable l : values) {
				sum = sum + l.get();

			}
			System.out.println(key + " " + sum);
			context.write(key, new LongWritable(sum));

		}
	}

	
}
