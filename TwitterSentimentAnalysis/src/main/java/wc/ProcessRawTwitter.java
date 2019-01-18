package wc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

public class ProcessRawTwitter {
	
	public static void main(String[] args) throws IOException
	{
		File folder = new File(args[0]);
		String csv = ProcessRawTwitter.listFilesForFolder(folder);
		try (PrintWriter out = new PrintWriter("filename.txt")) {
		    out.println(csv);
		}
		
	}
	
	public static String listFilesForFolder(final File folder) throws IOException {
        String file = null;
	    for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	            listFilesForFolder(fileEntry);
	        } else {
	            System.out.println(fileEntry.getName());
	            file = ProcessRawTwitter.readFile(fileEntry.getName());
	        }
	    }
        return file;

	}
	
	
	public static String readFile(String fileName) throws IOException {
		System.out.println(fileName);
	    BufferedReader br = new BufferedReader(new FileReader(fileName));
	    try {
	        StringBuilder sb = new StringBuilder();
	        String line = br.readLine();
	        System.out.println(line);
	        while (line != null) {
	            sb.append(line);
	            sb.append(",");
	            line = br.readLine();
	        }
	        return sb.toString();
	    } finally {
	        br.close();
	    }
	}

}
