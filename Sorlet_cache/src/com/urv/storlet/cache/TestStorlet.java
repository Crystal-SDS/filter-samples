package com.urv.storlet.cache;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import com.ibm.storlet.common.StorletInputStream;
import com.ibm.storlet.common.StorletLogger;
import com.ibm.storlet.common.StorletObjectOutputStream;
import com.ibm.storlet.common.StorletOutputStream;

public class TestStorlet {
	
	public static final String OUTPUT_MD_FILE_NAME = "input/output_md.txt";
	public static final String LOGGER_FILE_NAME = "input/logger";	
	public static SSDCacheStorlet storlet = new SSDCacheStorlet();
	
	public static void main(String[] args) throws IOException {
		System.out.println("entering main");

		List<String> storedFiles = new ArrayList<>();
		Random random = new Random();
		String operation = "";
		String inputFile = "";
		String outputFile = "input/testOutput.txt";;
		for (int i = 0; i< 50; i++){
			//Decide if we want a PUT or a GET
			operation = "PUT";
			if (random.nextFloat()<0.5 && !storedFiles.isEmpty()) operation="GET";
			
			//In case of a PUT, decide if to get a new file or an existing one
			if (operation == "PUT") {
				inputFile = "input/test.txt";
				if (random.nextFloat()<0.5) {
					String newFileName = "input/" + UUID.randomUUID().toString();
					Files.copy(new File(inputFile).toPath(), new File(newFileName).toPath());
					inputFile = newFileName;
				} else if (!storedFiles.isEmpty()) inputFile = storedFiles.get(random.nextInt(storedFiles.size()));
				storedFiles.add(inputFile);				
			} else {
				inputFile = storedFiles.get(random.nextInt(storedFiles.size()));
			}			
			doInvoke(operation, inputFile, outputFile);
		}
		HashSet<String> setFiles = new HashSet<>();
		for (String s: storedFiles) 
			setFiles.add(s);

		System.out.println("EXPECTED CACHED FILES: " + setFiles.size());
		for (String s: setFiles) 
			System.out.println(s);

		System.out.println("exiting main");
	}
	
	public static void doInvoke(String operationType, String inputFile, String outputFile) {
		try {
			FileInputStream infile = new FileInputStream(inputFile);
			FileOutputStream outfile = new FileOutputStream(outputFile);
			FileOutputStream outfile_md = new FileOutputStream(OUTPUT_MD_FILE_NAME);
	
			HashMap<String, String> md = new HashMap<String, String>();
			StorletInputStream inputStream1 = new StorletInputStream(infile.getFD(), md);
	        StorletObjectOutputStream outStream = new StorletObjectOutputStream(outfile.getFD(), md, outfile_md.getFD());
	        
	        ArrayList<StorletInputStream> inputStreams = new ArrayList<StorletInputStream>();
	        inputStreams.add(inputStream1);	        
	        ArrayList<StorletOutputStream> outStreams = new ArrayList<StorletOutputStream>();
	        outStreams.add(outStream);
	        
			FileOutputStream loggerFile = new FileOutputStream(LOGGER_FILE_NAME);	
				
			StorletLogger logger = new StorletLogger(loggerFile.getFD());				
			Map<String, String> parameters = new HashMap<String, String>();	
	
			parameters.put("objectId", inputFile);
			parameters.put("requestType", operationType);
			
			System.out.println("before storlet");
			storlet.invoke(inputStreams, outStreams, parameters, logger);
			System.out.println("after storlet");
			
			infile.close();
			outfile.close();
			outfile_md.close();
			
			loggerFile.close();		
		} catch (Exception e) {
			System.out.println("had an exception");
			System.out.println(e.getMessage());
		}
	}
}