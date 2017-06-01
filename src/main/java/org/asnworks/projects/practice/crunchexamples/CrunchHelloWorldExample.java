package org.asnworks.projects.practice.crunchexamples;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;

/**
 * Hello world!
 *
 */
public class CrunchHelloWorldExample {
	public static void main(String[] args) {

		String inputFilePath = args[0];
		String outputPath = args[1];

		Pipeline pipeline = new MRPipeline(CrunchHelloWorldExample.class, new Configuration());
		PCollection personData = pipeline.readTextFile(inputFilePath);

		System.out.println("Number of lines ingested from : " + personData.length().getValue());

		PCollection personDataWithEmails = personData.parallelDo(AddEmailAddressDoFn(), Writables.strings());
		personDataWithEmails.write(To.textFile(outputPath));

		PipelineResult result = pipeline.done();
		System.exit(result.succeeded() ? 0 : 1);
	}

	static DoFn<String, String> AddEmailAddressDoFn() {
		return new DoFn<String, String>() {
			public void process(String input, Emitter emitter) {
				String[] inputParts = input.split(",");
				String personName = inputParts[0];
				String personAge = inputParts[1];
				String personCompany = inputParts[2];

				String personEmail = personName + "@" + personCompany + ".com";
				String updatePerson = input+","+personEmail;
				
				emitter.emit(updatePerson);
			}
		};
	}
}
