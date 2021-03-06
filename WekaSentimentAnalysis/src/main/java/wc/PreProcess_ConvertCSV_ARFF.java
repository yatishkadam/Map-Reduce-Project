package wc;

import weka.core.Instances;
import java.io.File;

import weka.core.converters.CSVLoader;

import weka.core.converters.ArffSaver;

/*
 * * Class to convert the training data for twitter sentiment analysis which is in
 * csv format to ARFF format  for weka classifiers
 */
public class PreProcess_ConvertCSV_ARFF {

	
	public static void main(String[] args) throws Exception {
	    
	    	CSVLoader loader = new CSVLoader();
	    	ArffSaver saver = new ArffSaver();

	    	loader.setSource(new File(args[0]));
	    	Instances data = loader.getDataSet();

	    	saver.setInstances(data);
	    	saver.setFile(new File(args[1]));
	    	saver.writeBatch();
	}
}
