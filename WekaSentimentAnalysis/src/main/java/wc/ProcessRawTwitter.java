package wc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class ProcessRawTwitter {
	
	public static void main(String[] args) throws IOException
	{
		File folder = new File(args[0]);
		String csv = ProcessRawTwitter.listFilesForFolder(folder);
		
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
	    BufferedReader br = new BufferedReader(new FileReader(fileName));
	    try {
	        StringBuilder sb = new StringBuilder();
	        String line = br.readLine();

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
