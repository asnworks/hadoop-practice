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
import org.apache.crunch.io.From;
import org.apache.crunch.io.avro.AvroFileTarget;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.asnworks.projects.practice.crunchexamples.domain.Sales;

import au.com.bytecode.opencsv.CSVParser;

public class CrunchSalesCSVToSalesAvro extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new CrunchSalesCSVToSalesAvro(), args);
		System.exit(result);
	}

	@Override
	public int run(String[] args) throws Exception {

		// final String fileInputPath = args[0];
		// final String fileOutputPath = args[1];

		final String fileInputPath = "SalesJan2009.csv";
		final String fileOutputPath = "outputs";

		final Configuration conf = getConf();

		// final Pipeline pipeline = new
		// MRPipeline(CrunchSalesCSVToSalesAvro.class, "ConvertSalesCSVToAvro",
		// conf);
		final Pipeline pipeline = MemPipeline.getInstance();
		final PCollection<String> csvStrinCollection = pipeline.readTextFile(fileInputPath);
		final PCollection<Sales> songCollection = csvStrinCollection.parallelDo("SalesCSVToSalesObjectDoFn",
				new SalesCSVToSalesObjectDoFn(), Avros.records(Sales.class));

		AvroFileTarget avroFileTarget = new AvroFileTarget(fileOutputPath);
		Path salesAvroPath = avroFileTarget.getPath();
		pipeline.write(songCollection, avroFileTarget);

		PCollection<Sales> salesObjectsFromAvro = pipeline
				.read(From.avroFile(salesAvroPath, Avros.records(Sales.class)));

		PTable<String, Double> table = salesObjectsFromAvro.parallelDo(new TotalSalesByCountryDoFn(),
				Writables.tableOf(Writables.strings(), Writables.doubles()));

		PTable<String, Double> finalResult = table.groupByKey().combineValues(Aggregators.SUM_DOUBLES());
		
		System.out.println(finalResult.toString());

		PipelineResult result = pipeline.done();
		return result.succeeded() ? 0 : 1;
	}

	static class TotalSalesByCountryDoFn extends DoFn<Sales, Pair<String, Double>> {

		private static final long serialVersionUID = -8326229547230564079L;

		@Override
		public void process(Sales input, Emitter<Pair<String, Double>> output) {
			output.emit(Pair.of(input.getCountry(), Double.parseDouble(input.getPrice())));
		}

	}

	static class SalesCSVToSalesObjectDoFn extends DoFn<String, Sales> {
		private static final long serialVersionUID = -6801821077297682998L;

		final static CSVParser CSV_PARSER = new CSVParser();

		@Override
		public void process(String input, Emitter<Sales> output) {
			try {

				final String[] inputParts = CSV_PARSER.parseLine(input);
				/*
				 * Transaction_date,Product,Price,Payment_Type,Name,City,State,
				 * Country,Account_Created,Last_Login,Latitude,Longitude
				 */
				final Sales.Builder sales = Sales.newBuilder();
				sales.setTransactionDate(inputParts[0]);
				sales.setProductId(inputParts[1]);
				sales.setPrice(inputParts[2]);
				sales.setPaymentType(inputParts[3]);
				sales.setName(inputParts[4]);
				sales.setCity(inputParts[5]);
				sales.setState(inputParts[6]);
				sales.setCountry(inputParts[7]);
				sales.setAccountCreated(inputParts[8]);
				sales.setLastLogin(inputParts[9]);
				sales.setLatitude(inputParts[10]);
				sales.setLongitude(inputParts[11]);

				output.emit(sales.build());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

}
