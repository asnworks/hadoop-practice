package org.asnworks.projects.practice.crunchexamples;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.asnworks.projects.practice.crunchexamples.domain.Song;

public class CrunchSongFilterFnExample extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		final int result = ToolRunner.run(new Configuration(), new CrunchSongFilterFnExample(), args);
		System.exit(result);
	}

	@Override
	public int run(String[] args) throws Exception {

		final String inputFileName = args[0];
		final String outputFileName = args[1];
		
		final Configuration conf = getConf();

		final Pipeline pipeline = new MRPipeline(CrunchSongFilterFnExample.class, "CrunchSongFilterExample",
				conf);
		final PCollection<Song> songCollection = pipeline.read(From.avroFile(inputFileName, Avros.records(Song.class)));
		final PCollection<Song> filteredSongs = songCollection.filter("SongFilter", new SongFilterDoFn());
		pipeline.write(filteredSongs, To.avroFile(outputFileName));

		final PipelineResult result = pipeline.done();

		return result.succeeded() ? 0 : 1;
	}

}
