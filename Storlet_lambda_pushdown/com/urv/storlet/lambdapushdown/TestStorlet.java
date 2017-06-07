package com.urv.storlet.lambdapushdown;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.ibm.storlet.common.StorletInputStream;
import com.ibm.storlet.common.StorletLogger;
import com.ibm.storlet.common.StorletObjectOutputStream;
import com.ibm.storlet.common.StorletOutputStream;

public class TestStorlet {
	
	public static final String INPUT_FILE_NAME = "input/meter_gen.csv";
	public static final String OUTPUT_FILE_NAME = "/dev/null"; //"input/meter.results";
	public static final String OUTPUT_MD_FILE_NAME = "input/output_record_md.txt";
	public static final String LOGGER_FILE_NAME = "input/logger";	
	
	public static void main(String[] args) {
		System.out.println("entering main");
		try {
			long inputBytes = new File(INPUT_FILE_NAME).length();
			
			FileInputStream infile = new FileInputStream(INPUT_FILE_NAME);
			FileOutputStream outfile = new FileOutputStream(OUTPUT_FILE_NAME);
			FileOutputStream outfile_md = new FileOutputStream(OUTPUT_MD_FILE_NAME);

			HashMap<String, String> md = new HashMap<String, String>();
			StorletInputStream inputStream1 = new StorletInputStream(infile.getFD(), md);
	        StorletObjectOutputStream outStream = new StorletObjectOutputStream(outfile.getFD(), md, outfile_md.getFD());
	        
	        ArrayList<StorletInputStream> inputStreams = new ArrayList<StorletInputStream>();
	        inputStreams.add(inputStream1);	        
	        ArrayList<StorletOutputStream> outStreams = new ArrayList<StorletOutputStream>();
	        outStreams.add(outStream);
	        
	        //LambdaPushdownStorlet storlet; //new NoopStorlet();
	        LambdaPushdownStorlet storlet = new LambdaPushdownStorlet();
	        
			FileOutputStream loggerFile = new FileOutputStream(LOGGER_FILE_NAME);					
			StorletLogger logger = new StorletLogger(loggerFile.getFD());				
			Map<String, String> parameters = new HashMap<String, String>();	
			
			//parameters.put("1-lambda", "java.util.function.Predicate<java.lang.String>|filter(s -> s.contains(\"Hamlet\"))");
			//parameters.put("2-map", "s -> s + \"1234563564545\"");
			//parameters.put("3-filter", "s -> s.contains(\"B\")");	
			//parameters.put("4-map", "s -> s + \"aaaaaaaa\"");
			//parameters.put("5-filter", "s -> s.contains(\"A\")");	
			
			System.out.println("before storlet");
			long iniTime = System.nanoTime();
			storlet.invoke(inputStreams, outStreams, parameters, logger);
			System.out.println("after storlet: " + ((inputBytes/1024./1024.)/((System.nanoTime()-iniTime)/1000000000.)) + "MBps");			
			
			infile.close();
			outfile.close();
			outfile_md.close();
			
			for (int i=0; i<5; i++){
			
				infile = new FileInputStream(INPUT_FILE_NAME);
				outfile = new FileOutputStream(OUTPUT_FILE_NAME);
				outfile_md = new FileOutputStream(OUTPUT_MD_FILE_NAME);
				inputStream1 = new StorletInputStream(infile.getFD(), md);
		        outStream = new StorletObjectOutputStream(outfile.getFD(), md, outfile_md.getFD());	        
		        inputStreams = new ArrayList<StorletInputStream>();
		        inputStreams.add(inputStream1);	        
		        outStreams = new ArrayList<StorletOutputStream>();
		        outStreams.add(outStream);
		        
		        System.out.println("before storlet");
				iniTime = System.nanoTime();
				storlet.invoke(inputStreams, outStreams, parameters, logger);
				System.out.println("after storlet: " + ((inputBytes/1024./1024.)/((System.nanoTime()-iniTime)/1000000000.)) + "MBps");
		        
		        infile.close();
				outfile.close();
				outfile_md.close();
			}
			
			loggerFile.close();
			
		}
		catch (Exception e) {
			System.out.println("had an exception");
			System.out.println(e.getMessage());
		}
		System.out.println("exiting main");
	}


}
