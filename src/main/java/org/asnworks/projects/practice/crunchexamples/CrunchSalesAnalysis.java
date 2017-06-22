package org.asnworks.projects.practice.crunchexamples;

import java.io.IOException;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.asnworks.projects.practice.crunchexamples.domain.Sales;

import au.com.bytecode.opencsv.CSVParser;

public class CrunchSalesAnalysis extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new CrunchSalesAnalysis(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		final String inputPath = "SalesJan2009.csv";
		// final String outputPath = "";

		Pipeline pipeline = MemPipeline.getInstance();
		PCollection<String> lines = pipeline.readTextFile(inputPath);

		PTable<String, Double> p = lines.parallelDo(new SalesToString1(),
				Writables.tableOf(Writables.strings(), Writables.doubles()));


		PGroupedTable<String, Double> g = p.groupByKey();

		PTable<String, Double> finalList = g.combineValues(Aggregators
				.SUM_DOUBLES());
		System.out.println(finalList.toString());
		

		PipelineResult result = pipeline.done();
		return result.succeeded() ? 0 : 1;
	}

	static class SalesToString1 extends DoFn<String, Pair<String, Double>> {

		private static final long serialVersionUID = 2713802748986743338L;

		@Override
		public void process(String input, Emitter<Pair<String, Double>> emitter) {
			final CSVParser CSV_PARSER = new CSVParser();
			try {
				final String[] parts = CSV_PARSER.parseLine(input);

				String price = parts[2];
				String country = parts[7];

				emitter.emit(Pair.of(country, Double.parseDouble(price)));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	static class SalesToString extends DoFn<String, String> {

		private static final long serialVersionUID = 6425220356792709699L;

		@Override
		public void process(String input, Emitter<String> output) {
			final CSVParser CSV_PARSER = new CSVParser();
			try {
				final String[] parts = CSV_PARSER.parseLine(input);

				String price = parts[2];
				String country = parts[7];

				output.emit(country + "," + price);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	static class SalesCsvToAvroFileDoFn extends DoFn<String, Sales> {
		private static final long serialVersionUID = 7252434020422600482L;

		@Override
		public void process(String input, Emitter<Sales> output) {
			final CSVParser CSV_PARSER = new CSVParser();
			try {
				// Transaction_date,Product,Price,Payment_Type,Name,City,State,Country,Account_Created,Last_Login,Latitude,Longitude

				final String[] parts = CSV_PARSER.parseLine(input);

				final Sales.Builder sales = Sales.newBuilder();
				sales.setTransactionDate(parts[0]);
				sales.setProductId(parts[1]);
				//sales.setPrice(Double.parseDouble(parts[2]));
				sales.setPaymentType(parts[3]);
				sales.setName(parts[4]);
				sales.setCity(parts[5]);
				sales.setState(parts[6]);
				sales.setCountry(parts[7]);
				sales.setAccountCreated(parts[8]);
				sales.setLastLogin(parts[9]);
				sales.setLatitude(parts[10]);
				sales.setLongitude(parts[11]);

				output.emit(sales.build());

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

}
