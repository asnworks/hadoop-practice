package org.asnworks.projects.practice.crunchexamples;

import java.io.IOException;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.asnworks.projects.practice.crunchexamples.domain.Song;

import au.com.bytecode.opencsv.CSVParser;

public class SongExtractorDoFn extends DoFn<String, Song>{

	
	private static final long serialVersionUID = -3077072852952836463L;
	final static CSVParser PARSER = new CSVParser();
	
	@Override
	public void process(String input, Emitter<Song> output) {
		String[] inputLine = null;
		try {
			inputLine = PARSER.parseLine(input);
			System.out.println("===="+inputLine);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		
		final Song.Builder song = Song.newBuilder();
		song.setSongId(inputLine[0]);
		song.setSongTitle(inputLine[1]);
		System.out.println("Input Line : "+inputLine[2]);
		final String[] artistNames = inputLine[2].split("ft. ");
		System.out.println("Array length : "+artistNames.length);
		for(final String artistName : artistNames) {
			song.setArtistName(artistName);
			System.out.println("After split Artist name : "+artistName);			
			output.emit(song.build());
		}
		
	}
} 