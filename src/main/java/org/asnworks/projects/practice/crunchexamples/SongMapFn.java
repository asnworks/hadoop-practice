package org.asnworks.projects.practice.crunchexamples;

import java.io.IOException;

import org.apache.crunch.MapFn;
import org.asnworks.projects.practice.crunchexamples.domain.Song;

import au.com.bytecode.opencsv.CSVParser;

public class SongMapFn extends MapFn<String, Song> {
	
	private static final long serialVersionUID = 3562260687066425940L;
	private static CSVParser PARSER = new CSVParser();

	@Override
	public Song map(String input) {
		final Song.Builder song = Song.newBuilder();

		try {
			String[] parsedInputs = PARSER.parseLine(input);

			song.setSongId(parsedInputs[0]);
			song.setSongTitle(parsedInputs[1]);
			song.setArtistName(parsedInputs[2]);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return song.build();
	}

}
