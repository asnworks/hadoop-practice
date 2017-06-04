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

public class CrunchSongMapFnExample extends Configured implements Tool{

	public static void main(String[] args) throws Exception {

		final int response = ToolRunner.run(new Configuration(), new CrunchSongMapFnExample(),args);
		System.exit(response);
	}

	@Override
	public int run(String[] args) throws Exception {
		final Configuration conf = getConf();
		
		String inputFileName = args[0];
		String outputFileName = args[1];

		final Pipeline pipeline = new MRPipeline(CrunchSongMapFnExample.class, "SongExtractor", conf);
		final PCollection<String> songsAsStrings = pipeline.readTextFile(inputFileName);

		final PCollection<Song> songObjects = songsAsStrings.parallelDo("Song object from String", new SongMapFn(),
				Avros.records(Song.class));

		pipeline.write(songObjects, new AvroFileTarget(outputFileName));

		final PipelineResult result = pipeline.done();
		return result.succeeded() ? 0 : 1;
	}

}
