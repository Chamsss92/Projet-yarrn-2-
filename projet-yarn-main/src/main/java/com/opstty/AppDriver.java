package com.opstty;

import org.apache.hadoop.util.ProgramDriver;

import com.opstty.job.DistrictsTrees;
import com.opstty.job.MaxHeightSpecies;
import com.opstty.job.OldestDistrictTreeSort;
import com.opstty.job.TreeSpecies;
import com.opstty.job.TreeSpeciesCount;
import com.opstty.job.TreesSortedByHeight;
import com.opstty.job.WordCount;
import com.opstty.reducer.OldestDistrictReduce;

public class AppDriver {
	public static void main(String argv[]) {
		int exitCode = -1;
		ProgramDriver programDriver = new ProgramDriver();

		try {
			programDriver.addClass("wordcount", WordCount.class,
					"A MapReduce program wich counts the words in the input files.");

			programDriver.addClass("DistrictsTrees", DistrictsTrees.class,
					"A MapReduce program wich returns the distinct districts with trees in the Remarkable Trees of Paris dataset.");

			programDriver.addClass("treeSpecies", TreeSpecies.class,
					"A MapReduce program wich returns the distinct tree species in the Remarkable Trees of Paris dataset.");

			programDriver.addClass("treeSpeciesCount", TreeSpeciesCount.class,
					"A MapReduce program wich returns the distinct tree species (and the number of trees for each one) in the Remarkable Trees of Paris dataset.");

			programDriver.addClass("maxHeightSpecies", MaxHeightSpecies.class,
					"A MapReduce program wich returns the highest height of trees per species in the Remarkable Trees of Paris dataset.");

			programDriver.addClass("treesSortedByHeight", TreesSortedByHeight.class,
					"A MapReduce program wich returns all the trees in the Remarkable Trees of Paris dataset, sorted by height.");

			programDriver.addClass("oldestTreeDistrictSort", OldestDistrictTreeSort.class,
					"A MapReduce program wich returns the district(s) with the oldest tree(s) in the Remarkable Trees of Paris dataset, using a sort.");

			programDriver.addClass("OldestDistrictReduce", OldestDistrictReduce.class,
					"A MapReduce program wich returns the district(s) with the oldest tree(s) in the Remarkable Trees of Paris dataset, checking through all the data.");

			exitCode = programDriver.run(argv);
		} catch (Throwable throwable) {
			throwable.printStackTrace();
		}

		System.exit(exitCode);
	}
}
