package org.asnworks.projects.practice.crunchexamples;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;

public class MemPipelineTest {

	public static void main(String[] args) {

		String inputFileName = "SalesJan2009.csv";
		String outputFileName = "output.csv";

		Pipeline pipeline = MemPipeline.getInstance();
		PCollection<String> pCollection = pipeline.readTextFile(inputFileName);

		PCollection<String> outputCollection = pCollection.parallelDo(TestDoFn(), Writables.strings());

		pipeline.writeTextFile(outputCollection, outputFileName);

		PipelineResult result = pipeline.done();
		int r = result.succeeded() ? 0 : 1;
		System.exit(r);

	}

	static DoFn<String, String> TestDoFn() {
		return new DoFn<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void process(String input, Emitter<String> output) {
				String[] parts = input.split(",");

				output.emit(parts[0] + "," + parts[1] + "," + parts[2]);
			}
		};
	}

}
