package org.asnworks.projects.practice.hadoopexamples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Hello world!
 *
 */
public class HadoopWriteAndReadFileExample {
	private static final String fileName = "hello.txt";
	private static final String message = "Hello, world";
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Path fileNamePath = new Path(fileName);
		if(fs.exists(fileNamePath)) {
			fs.delete(fileNamePath, true);
		}
		
		FSDataOutputStream out = fs.create(fileNamePath);
		out.writeUTF(message);
		out.close();
		
		FSDataInputStream in = fs.open(fileNamePath);
		String message = in.readUTF();
		in.close();
		System.out.println("The Message from HDFS is : "+message);
		
	}
}
