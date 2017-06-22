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

public class CrunchSkillSpeedSecondFieldLessThan20MoreThan50Example extends
		Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int r = ToolRunner.run(new Configuration(),
				new CrunchSkillSpeedSecondFieldLessThan20MoreThan50Example(),
				args);
		System.exit(r);
	}

	@Override
	public int run(String[] args) throws Exception {
		final String inputFileName = "Module_4_Ex2.txt";

		final Pipeline pipeline = MemPipeline.getInstance();
		pipeline.enableDebug();

		final PCollection<String> lines = pipeline.read(From
				.textFile(inputFileName));

		final PCollection<String> filterColumns = lines.parallelDo(
				new SplitCSVSecondColDoFn(), Writables.strings());

		System.out.println("Records where second field is < 10 or > 50 : "+filterColumns.toString());

		final PipelineResult result = pipeline.done();
		return result.succeeded() ? 0 : 1;
	}

	static class SplitCSVSecondColDoFn extends DoFn<String, String> {
		private static final long serialVersionUID = -2908965445317460184L;

		@Override
		public void process(String input, Emitter<String> emitter) {
			final String[] parts = input.split("\t");
			int secondCol = Integer.parseInt(parts[1]);

			if (secondCol < 20 || secondCol > 50) {
				emitter.emit(String.valueOf(secondCol));
			}
		}

	}

}
