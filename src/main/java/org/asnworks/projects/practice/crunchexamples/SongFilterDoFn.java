package org.asnworks.projects.practice.crunchexamples;

import org.apache.crunch.FilterFn;
import org.asnworks.projects.practice.crunchexamples.domain.Song;

public class SongFilterDoFn extends FilterFn<Song> {

	private static final long serialVersionUID = -2049976107957257434L;

	@Override
	public boolean accept(Song input) {
		if (input.getArtistName().startsWith("C")) {
			return true;
		} else {
			return false;
		}
	}

}
