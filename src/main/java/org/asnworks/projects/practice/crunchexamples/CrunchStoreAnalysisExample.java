package org.asnworks.projects.practice.crunchexamples;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.lib.Join;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CrunchStoreAnalysisExample extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int r = ToolRunner.run(new Configuration(),
				new CrunchStoreAnalysisExample(), args);
		System.exit(r);
	}

	@Override
	public int run(String[] args) throws Exception {

		final String inputFileStoreDetails = "store_details";
		final String inputFileStoreSales = "store_sales";

		final Pipeline pipeline = MemPipeline.getInstance();

		final PCollection<String> storeDetailsCollection = pipeline.read(From
				.textFile(inputFileStoreDetails));
		final PCollection<String> storeSalesCollection = pipeline.read(From
				.textFile(inputFileStoreSales));

		final PTable<String, String> storeDetailsTable = storeDetailsCollection
				.parallelDo(
						new ExtractStoreDetailsDoFn(),
						Writables.tableOf(Writables.strings(),
								Writables.strings()));

		final PTable<String, String> storeSalesTable = storeSalesCollection
				.parallelDo(
						new ExtractStoreSalesDoFn(),
						Writables.tableOf(Writables.strings(),
								Writables.strings()));
		
		

		System.out.println(Join.innerJoin(storeDetailsTable, storeSalesTable)
				.toString());

		final PipelineResult result = pipeline.done();
		return result.succeeded() ? 0 : 1;
	}

	static class ExtractStoreDetailsDoFn extends
			DoFn<String, Pair<String, String>> {
		private static final long serialVersionUID = 8264357631337793165L;

		@Override
		public void process(String input, Emitter<Pair<String, String>> emitter) {
			final String[] parts = input.split(",");
			emitter.emit(Pair.of(parts[0], parts[1] + "," + parts[2]));
		}

	}

	static class ExtractStoreSalesDoFn extends
			DoFn<String, Pair<String, String>> {
		private static final long serialVersionUID = 4478031572431791461L;

		@Override
		public void process(String input, Emitter<Pair<String, String>> emitter) {
			final String[] parts = input.split(",");
			emitter.emit(Pair.of(parts[0], ""));
		}

	}

}
