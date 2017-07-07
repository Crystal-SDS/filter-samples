package com.urv.storlet.lambdapushdown;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.openstack.storlet.common.StorletInputStream;
import org.openstack.storlet.common.StorletLogger;
import org.openstack.storlet.common.StorletObjectOutputStream;
import org.openstack.storlet.common.StorletOutputStream;

public class TestStorlet {
	
	public static final String INPUT_FILE_NAME = "input/meter-new-15MB.csv";
	public static final String OUTPUT_FILE_NAME = "input/meter.results"; // "/dev/null"; //
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
			"".replace("", "");
			
			//selected_columns=0|1|5|7, where_clause=And(StringStartsWith(0|2015-01)|EqualTo(7|Paris))
			//parameters.put("0-lambda", "None<>|skip(1)");
			parameters.put("0-lambda", "java.util.function.Function<java.lang.String' java.util.List<java.lang.String>>|"
				+ "map(s -> { java.util.List<String> l $ new java.util.ArrayList<String>(); String[] a $ s.split(\"'\"); "
				+ "l.add(a[0]); l.add(a[1]); l.add(a[5]); l.add(a[7]);"
				+ "return l; })");
			parameters.put("1-lambda", "java.util.function.Predicate<java.util.List<java.lang.String>>|"
					+ "filter(s -> (s.get(0).startsWith(\"2015-01\") && s.get(3).equals(\"Paris\")) || s.get(0).startsWith(\"date\"))");
			parameters.put("2-lambda", "java.util.function.Function<java.util.List<java.lang.String>' java.lang.String>|"
					+ "map(l -> l.toString().replace(\"[\", \"\").replace(\"]\", \"\"))");
			
			/*
			 * 1-lambda=java.util.function.Function<java.lang.String' java.util.List<java.lang.String>>|map(s -> { java.util.List<String> l $ new java.util.ArrayList<String>(); String[] a $ s.split("'");
				l.add(a[0]); l.add(a[1]); l.add(a[5]); l.add(a[7]); return l; })
			 * 
			 * 
			 * */
			
			
			//parameters.put("1-lambda", "java.util.function.Predicate<java.lang.String>|filter(s -> s.contains(\"Hamlet\"))");
			//parameters.put("0-lambda", "java.util.function.Predicate<java.lang.String>|filter(s -> s.startsWith(\"storage_done\") "
			//		+ "&& (s.contains(\"PutContentResponse\") || s.contains(\"GetContentResponse\") || s.contains(\"MakeResponse\") || "
			//		+ "s.contains(\"Unlink\") || s.contains(\"MoveResponse\")))");
			//parameters.put("1-lambda", "java.util.function.Function<java.lang.String' java.lang.String[]>|"
			//		+ "map(s -> s.split(\",\"))");
			//parameters.put("1-lambda", "java.util.function.Function<java.lang.String' java.util.List<java.lang.String>>|"
			//		+ "map(s -> { java.util.List<String> l $ new java.util.ArrayList<String>(); String[] a $ s.split(\"'\"); for (String x : a) l.add(x); return l; })");
			//parameters.put("2-lambda", "java.util.function.Predicate<java.util.List<java.lang.String>>|filter(s -> s.get(19).equals(\"PutContentResponse\") "
			//		+ "|| s.get(19).equals(\"GetContentResponse\") || s.get(19).equals(\"MakeResponse\") || s.get(19).equals(\"Unlink\") || "
			//		+ "s.get(19).equals(\"MoveResponse\"))");	
			//parameters.put("3-lambda", "java.util.function.Function<java.util.List<java.lang.String>\' "
			//		+ "java.util.AbstractMap.SimpleEntry<java.lang.String\' java.lang.Integer>>|"
			//		+ "map(s -> new SimpleEntry<String\' Integer>(s.get(33) + \"-\" + s.get(19)\' 1))");
			//parameters.put("4-lambda", "Collector|collect(java.util.stream.Collectors.groupingBy("
			//		+ "SimpleEntry<String' Integer>::getKey' java.util.stream.Collectors.counting()))");

			//parameters.put("1-lambda", "java.util.function.Predicate<java.lang.String>|filter(s -> s.contains(\"Hamlet\"))");	
			
			System.out.println("before storlet");
			long iniTime = System.nanoTime();
			storlet.invoke(inputStreams, outStreams, parameters, logger);
			System.out.println("after storlet: " + ((inputBytes/1024./1024.)/((System.nanoTime()-iniTime)/1000000000.)) + "MBps");			
			
			infile.close();
			outfile.close();
			outfile_md.close();
			
			for (int i=0; i<1; i++){
			
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
