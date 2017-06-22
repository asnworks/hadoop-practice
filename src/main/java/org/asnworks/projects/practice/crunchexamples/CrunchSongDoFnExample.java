package org.asnworks.projects.practice.crunchexamples;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.avro.AvroFileTarget;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.asnworks.projects.practice.crunchexamples.domain.Song;

public class CrunchSongDoFnExample extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		
		int result = ToolRunner.run(new Configuration(), new CrunchSongDoFnExample(), args);
		System.exit(result);

	}

	@Override
	public int run(String[] args) throws Exception {
		
		final String inputPath = args[0];
		final String outputPath = args[1];
		
		final Configuration conf =getConf();
		
		Pipeline pipeline = new MRPipeline(CrunchSongDoFnExample.class, "SongDoFnExample", conf);
		PCollection<String> inputLineCollection = pipeline.readTextFile(inputPath);
		
		PCollection<Song> songObjects = inputLineCollection.parallelDo("SongObjectsFromString", new SongExtractorDoFn(), Avros.records(Song.class));
		
		pipeline.write(songObjects, new AvroFileTarget(outputPath));
		PipelineResult result = pipeline.done();
		
		return result.succeeded() ? 0 : 1;
	}

}
