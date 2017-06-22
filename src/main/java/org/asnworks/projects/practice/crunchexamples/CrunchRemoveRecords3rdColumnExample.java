package org.asnworks.projects.practice.crunchexamples;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CrunchRemoveRecords3rdColumnExample extends Configured implements
		Tool {

	public static void main(String[] args) throws Exception {
		int r = ToolRunner.run(new Configuration(),
				new CrunchRemoveRecords3rdColumnExample(), args);

		System.exit(r);
	}

	@Override
	public int run(String[] args) throws Exception {
		final String inputFile = "Module3_Ext1.txt";

		final Pipeline pipeline = MemPipeline.getInstance();
		pipeline.enableDebug();

		final PCollection<String> lines = pipeline.read(From
				.textFile(inputFile));

		final PCollection<String> collection = lines.parallelDo(
				new SplitCSVDoFn(), Writables.strings());

		for (String line : collection.materialize()) {
			System.out.println(line);
		}

		PipelineResult result = pipeline.done();

		return result.succeeded() ? 0 : 1;
	}

	static class SplitCSVDoFn extends DoFn<String, String> {
		private static final long serialVersionUID = 6936062855413769917L;

		@Override
		public void process(String input, Emitter<String> emitter) {
			final String[] parts = input.split("\t");
			String thirdColumn = parts[2];

			if (Double.parseDouble(thirdColumn) < 20) {
				emitter.emit(input);
			}
		}

	}

}
