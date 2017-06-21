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
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CrunchSkillSpeedCountSumMaxMinExample extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int response = ToolRunner.run(new Configuration(), new CrunchSkillSpeedCountSumMaxMinExample(), args);
		System.exit(response);
	}

	@Override
	public int run(String[] args) throws Exception {

		final String inputFile = "Module3_Ext1.txt";

		final Pipeline pipeline = MemPipeline.getInstance();
		pipeline.enableDebug();

		final PCollection<String> lines = pipeline.readTextFile(inputFile);

		PTable<String, Double> table = lines.parallelDo(new SplitKeyValueDoFn(),
				Writables.tableOf(Writables.strings(), Writables.doubles()));

		System.out.println("CSV Data");
		for (Pair<String, Double> element : table.materialize()) {
			System.out.println(element.toString());
		}

		PTable<String, Double> sum = table.groupByKey().combineValues(Aggregators.SUM_DOUBLES());

		System.out.println("Sum of each value for the Key : " + sum.toString());

		PTable<String, Double> max = table.groupByKey().combineValues(Aggregators.MAX_DOUBLES());

		System.out.println("Max of each value for the Key : " + max.toString());

		PTable<String, Double> min = table.groupByKey().combineValues(Aggregators.MIN_DOUBLES());

		System.out.println("Min of each value for the Key : " + min.toString());

		PTable<String, Long> count = Aggregate.count(table.parallelDo(new FindCountDoFn(), Writables.strings()));

		System.out.println("Counts for the each Key : " + count.toString());

		final PipelineResult result = pipeline.done();

		return result.succeeded() ? 0 : 1;
	}

	static class FindCountDoFn extends DoFn<Pair<String, Double>, String> {
		private static final long serialVersionUID = -5999764081675390361L;

		@Override
		public void process(Pair<String, Double> input, Emitter<String> emitter) {
			emitter.emit(input.first());
		}

	}

	static class SplitKeyValueDoFn extends DoFn<String, Pair<String, Double>> {
		private static final long serialVersionUID = 3312584912928850820L;

		@Override
		public void process(String input, Emitter<Pair<String, Double>> emitter) {
			final String[] parts = input.split("\t");
			String key = parts[0];
			emitter.emit(Pair.of(key, Double.parseDouble(parts[2])));
		}

	}

}