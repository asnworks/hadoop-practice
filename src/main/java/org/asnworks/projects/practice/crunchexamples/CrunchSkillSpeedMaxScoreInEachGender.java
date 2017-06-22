package org.asnworks.projects.practice.crunchexamples;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CrunchSkillSpeedMaxScoreInEachGender extends Configured implements
		Tool {

	public static void main(String[] args) throws Exception {
		int r = ToolRunner.run(new Configuration(),
				new CrunchSkillSpeedMaxScoreInEachGender(), args);
		System.exit(r);
	}

	@Override
	public int run(String[] args) throws Exception {

		final String inputFileName = "Module_4_Ex1.txt";

		final Pipeline pipeline = MemPipeline.getInstance();
		pipeline.enableDebug();

		final PCollection<String> lines = pipeline.read(From
				.textFile(inputFileName));

		final PTable<String, Integer> table = lines.parallelDo(
				new SplitCSVGenderAndScoreDoFn(),
				Writables.tableOf(Writables.strings(), Writables.ints()));

		System.out.println(table);

		System.out.println("Maximum Scorer in each Gender : "
				+ table.groupByKey().combineValues(Aggregators.MAX_INTS()));
		System.out.println("Minimum Scorer in each Gender : "
				+ table.groupByKey().combineValues(Aggregators.MIN_INTS()));

		PipelineResult result = pipeline.done();
		return result.succeeded() ? 0 : 1;
	}

	static class SplitCSVGenderAndScoreDoFn extends DoFn<String, Pair<String, Integer>> {

		private static final long serialVersionUID = -3712300753328131393L;

		@Override
		public void process(String input, Emitter<Pair<String, Integer>> emitter) {
			final String[] parts = input.split("\t");
			emitter.emit(Pair.of(parts[2], Integer.parseInt(parts[1])));
		}

	}
	
	

}
