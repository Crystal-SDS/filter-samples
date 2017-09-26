package com.urv.storlet.lambdapushdown;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.ibm.storlet.common.StorletException;
import com.ibm.storlet.common.StorletInputStream;
import com.ibm.storlet.common.StorletLogger;
import com.ibm.storlet.common.StorletObjectOutputStream;
import com.ibm.storlet.common.StorletOutputStream;

public class TestStorlet {
	
	public static final String INPUT_FILE_NAME = "input/apache.log";//"input/meter-new-15MB.csv";
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
			
			//selected_columns=0|1|5|7, where_clause=And(StringStartsWith(0|2015-01)|EqualTo(7|Paris))
			//parameters.put("add_header", "true");		
			parameters.put("0-lambda", "java.util.function.Function<java.lang.String' java.util.List<java.lang.String>>|"
					+ "map(s -> { java.util.List<String> l $ new java.util.ArrayList<String>(); String[] a $ s.split(\" \"); "
							+ "for (String x : a) l.add(x); return l; })");
			parameters.put("1-lambda", "java.util.function.Predicate<java.util.List<java.lang.String>>|"
					+ "filter(split -> (split.size() $$ 9 && (split.get(8).startsWith(\"40\") || "
					+ "split.get(8).startsWith(\"50\"))))");
			parameters.put("2-lambda", "java.util.function.Function<java.util.List<java.lang.String>' java.lang.String>|"
					+ "map(split -> split.get(3).substring(1' split.get(3).length()))");
			parameters.put("3-lambda", "java.util.function.Function<java.lang.String' java.lang.Integer>|"
					+ "map(s -> { try { return "
									+ " (int) new SimpleDateFormat(\"dd/MMM/yyyy:hh:mm:ss\").parse(s).getTime() / 3600000 - 223321; "
							 + "}catch(Exception e){e.printStackTrace();}return null;})");
			parameters.put("4-lambda", "java.util.function.Function<java.lang.Integer' java.util.AbstractMap.SimpleEntry<java.lang.Integer' java.lang.Integer>>|"
					+ "map(l -> new SimpleEntry<Integer' Integer>(l' 1))");
			parameters.put("5-lambda", "Collector|"
					+ "collect(java.util.stream.Collectors.groupingBy(SimpleEntry<Integer' Integer>::getKey' java.util.stream.Collectors.counting()))");
			
			
			/*parameters.put("0-lambda", "java.util.function.Function<java.lang.String' java.util.List<java.lang.String>>|" +
				"map(s -> { java.util.List<String> list = new java.util.ArrayList<String>(); " +
				"String[] a = s.split(\",\"); list.add(a[0]); list.add(a[1]); list.add(\"\"); list.add(\"\"); " +
				"list.add(a[4]); list.add(a[5]); list.add(\"\"); list.add(\"\"); list.add(\"\"); list.add(a[9]); list.add(a[10]);" + 
				"return list;})");
			parameters.put("1-lambda", "java.util.function.Predicate<java.util.List<java.lang.String>>|" +
				"filter(s -> s.get(0).startsWith(\"2012\") && s.get(4).equals(\"elec\"))");	*/		
			/*parameters.put("2-lambda", "java.util.function.BinaryOperator<java.util.List<java.lang.String>>|" +
			"reduce((l1, l2) -> { if (Double.valueOf(l1.get(1)) > Double.valueOf(l2.get(1))) return l1; " +
			"else return l2;})");*/
			
			
			/*parameters.put("0-lambda", "java.util.function.Function<java.lang.String' java.util.List<java.lang.String>>|"
				+ "map(s -> { java.util.List<String> l $ new java.util.ArrayList<String>(); String[] a $ s.split(\"'\"); "
				+ "l.add(a[0]); l.add(a[1]); l.add(\"\"); l.add(\"\"); l.add(\"\"); l.add(a[5]); l.add(\"\"); l.add(a[7]);"
				+ "l.add(\"\"); l.add(\"\"); l.add(\"\"); return l; })");
			parameters.put("1-lambda", "java.util.function.Predicate<java.util.List<java.lang.String>>|"
					+ "filter(s -> s.get(0).startsWith(\"2015-01\") && s.get(7).equals(\"Paris\"))");
			*/
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
			inputBytes = new File(INPUT_FILE_NAME).length();
			System.out.println("before storlet");
			long iniTime = System.nanoTime();
			storlet.invoke(inputStreams, outStreams, parameters, logger);
			System.out.println("after storlet: " + ((inputBytes/1024./1024.)/((System.nanoTime()-iniTime)/1000000000.)) + "MBps");			
			
			infile.close();
			outfile.close();
			outfile_md.close();
			
			for (int i=0; i<4; i++){
			
				infile = new FileInputStream(INPUT_FILE_NAME);
				outfile = new FileOutputStream(OUTPUT_FILE_NAME + String.valueOf(i));
				outfile_md = new FileOutputStream(OUTPUT_MD_FILE_NAME + String.valueOf(i) );
				inputStream1 = new StorletInputStream(infile.getFD(), md);
		        outStream = new StorletObjectOutputStream(outfile.getFD(), md, outfile_md.getFD());	        
		        inputStreams = new ArrayList<StorletInputStream>();
		        inputStreams.add(inputStream1);	        
		        outStreams = new ArrayList<StorletOutputStream>();
		        outStreams.add(outStream);
		        
		        StorletExecutionThread r = new StorletExecutionThread(inputStreams,outStreams, logger, parameters, new LambdaPushdownStorlet());
		        new Thread(r).start();
		        
		        //infile.close();
				//outfile.close();
				//outfile_md.close();
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

class StorletExecutionThread implements Runnable {
	
	ArrayList<StorletInputStream> inputStreams;
	ArrayList<StorletOutputStream> outputStreams;
	StorletLogger logger;				
	Map<String, String> parameters;
	LambdaPushdownStorlet storlet;
	
	public StorletExecutionThread(ArrayList<StorletInputStream> inputStreams,
			ArrayList<StorletOutputStream> outputStreams, StorletLogger logger, Map<String, String> parameters,
			LambdaPushdownStorlet storlet) {
		this.inputStreams = inputStreams;
		this.outputStreams = outputStreams;
		this.logger = logger;
		this.parameters = parameters;
		this.storlet = storlet;
	}

	public void run() {
		long inputBytes = new File(TestStorlet.INPUT_FILE_NAME).length();
		System.out.println("before storlet");
		long iniTime = System.nanoTime();
		try {
			storlet.invoke(inputStreams, outputStreams, parameters, logger);
		} catch (StorletException e) {
			e.printStackTrace();
		}
		System.out.println("after storlet: " + ((inputBytes/1024./1024.)/((System.nanoTime()-iniTime)/1000000000.)) + "MBps");
	}
}
