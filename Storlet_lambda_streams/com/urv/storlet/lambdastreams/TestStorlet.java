package com.urv.storlet.lambdastreams;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.openstack.storlet.common.StorletInputStream;
import org.openstack.storlet.common.StorletLogger;
import org.openstack.storlet.common.StorletObjectOutputStream;
import org.openstack.storlet.common.StorletOutputStream;
import com.urv.storlet.lambdastreams.examples.LambdaFilterGridPocketStorlet;
import com.urv.storlet.lambdastreams.examples.LambdaNoopStorlet;

public class TestStorlet {
	
	public static final String INPUT_FILE_NAME = "input/meter_gen-20170206162236.csv";
	public static final String OUTPUT_FILE_NAME = "input/test.results";
	public static final String OUTPUT_MD_FILE_NAME = "input/output_record_md.txt";
	public static final String LOGGER_FILE_NAME = "input/logger";	
	
	public static void main(String[] args) {
		System.out.println("entering main");
		try {

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
	        
	        //NoopStorlet storlet = new NoopStorlet();
	        LambdaStreamsStorlet storlet = new LambdaFilterGridPocketStorlet();
	        
			FileOutputStream loggerFile = new FileOutputStream(LOGGER_FILE_NAME);					
			StorletLogger logger = new StorletLogger(loggerFile.getFD());				
			Map<String, String> parameters = new HashMap<String, String>();	
			
			System.out.println("before storlet");
			storlet.invoke(inputStreams, outStreams, parameters, logger);
			System.out.println("after storlet");			
			
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
				storlet.invoke(inputStreams, outStreams, parameters, logger);
				System.out.println("after storlet");
		        
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
