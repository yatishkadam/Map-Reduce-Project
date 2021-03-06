package wc;

import weka.core.Instances;
import java.io.File;
import java.io.IOException;

import weka.core.converters.CSVLoader;
import weka.core.converters.CSVSaver;
import weka.core.converters.ArffLoader;
import weka.core.converters.ArffSaver;

/*
 * * Class to convert the weka classifier output which will be in ARFF back to csv
 */
public class PreProcess_ARFF_CSV {

	public static void main(String[] args) throws IOException {

	    ArffLoader loader = new ArffLoader();
	    CSVSaver saver = new CSVSaver();

	    loader.setSource(new File(args[0]));
	    Instances data = loader.getDataSet();

	    saver.setInstances(data);
	    saver.setFile(new File(args[1]));
	    saver.writeBatch();
		
	}

}
