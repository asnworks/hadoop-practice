package org.asnworks.projects.practice.hadoopexamples;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;

public class ExternalIdMapCreator {

	public static void main(String[] args) {
		try {
			process();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void process() throws IOException {
		String input = "SalesJan2009.csv";

		Reader r = new InputStreamReader(new FileInputStream(input),
				Charset.forName("utf8"));

		CSVReader reader = new CSVReader(r, '|',
				CSVParser.DEFAULT_QUOTE_CHARACTER, '\0');
		String[] line = reader.readNext();// Skip first line

		while ((line = reader.readNext()) != null) {
			String parts[] = line[0].split("\t");
			System.out.println(parts[0]);

		}

	}

}
