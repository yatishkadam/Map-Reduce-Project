package wc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

public class PreProcessRawTwitterData {
	private static String filePath = System.getProperty("user.dir");
	private static String inputFilePath = "C:\\Users\\yatish kadam\\Desktop\\Data_Mayhem\\TwitterSentimentAnalysis\\input";
	private static String outputFilePath = "C:\\Users\\yatish kadam\\Desktop\\Data_Mayhem\\TwitterSentimentAnalysis\\ouput\\";
	public static void main(String args[]) throws IOException{
		File folder = new File(inputFilePath);
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
		  File file = listOfFiles[i];
		  if (file.isFile()) {
		    //String content = FileUtils.readFileToString(file);
		    //System.out.println(content);
		    BufferedReader br = new BufferedReader(new FileReader(file));
		    try {
		        StringBuilder sb = new StringBuilder();
		        String line = br.readLine();
		        while (line != null) {
		            sb.append(line);
		            sb.append(",,");
		            line = br.readLine();
		        }
		        String content = sb.toString().replaceAll(",,\\*\\*\\*,,\\*\\*\\*,,",System.lineSeparator());
		        try{
					String[] contentList = Arrays.copyOfRange(content.split(",,"), 1, content.split(",,").length-1);
					content = String.join(",,", contentList);
				}
		        catch (Exception e){
		        	System.out.println(file.getName() + e.toString());
				}

		        if(content != ""){
					try (PrintWriter out = new PrintWriter(outputFilePath + file.getName()+ ".txt")) {
						out.println(content);
					}
				}

		    } finally {
		        br.close();
		    }
		}


		    }

}
}