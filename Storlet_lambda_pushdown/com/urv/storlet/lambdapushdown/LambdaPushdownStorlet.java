package com.urv.storlet.lambdapushdown;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.ibm.storlet.common.IStorlet;
import com.ibm.storlet.common.StorletException;
import com.ibm.storlet.common.StorletInputStream;
import com.ibm.storlet.common.StorletLogger;
import com.ibm.storlet.common.StorletObjectOutputStream;
import com.ibm.storlet.common.StorletOutputStream;

import main.java.pl.joegreen.lambdaFromString.LambdaFactory;
import main.java.pl.joegreen.lambdaFromString.TypeReference;

import static com.urv.storlet.lambdapushdown.StreamUtils.asStream;

/**
 * 
 * This Storlet is intended to dynamically execute code piggybacked into
 * HTTP headers on a data stream from a Swift object request. The idea
 * is to enable a more generic form of delegating computations to the
 * object store to improve the data ingestion problem of Big Data analytics.
 * 
 * This class performs three important tasks:
 * 
 * 1.- It transforms byte-level stream management of Swift objects into Java 8
 * streams, so applying functions (filter, map,..) to the data is much easier.
 * 
 * 2.- It enables to apply a list of functions to each Stream record, as well
 * as to define the order in which such functions will be applied.
 * 
 * 3.- It enables to dynamically compile the functions to be applied on records
 * as defined in the HTTP headers (based on lambdaFromString library).
 * 
 * Such functionality can enable frameworks like Spark to intelligently delegate
 * computations, such as data filtering and transformations, to the Swift cluster,
 * making the analytics jobs much faster.
 * 
 * @author Raul Gracia
 *
 */

public class LambdaPushdownStorlet implements IStorlet {
	
	protected static String DELIMITER = "\n";
	protected static String CHARSET = "UTF-8";
	
	//FIXME: To initialize the compiler, the library uses a dummy text file 
	//(helperClassTemplate.txt) that should at the moment exist to avoid errors
	protected static LambdaFactory lambdaFactory = LambdaFactory.get();
	
	/***
	 * Storlet invoke method. 
	 */
	@Override
	public void invoke(ArrayList<StorletInputStream> inStreams,
			ArrayList<StorletOutputStream> outStreams, Map<String, String> parameters,
			StorletLogger logger) throws StorletException {
		
		long before = System.nanoTime();
		logger.emitLog("----- Init Stream Record Reader Storlet -----");
		
		//Get streams and parameters
		StorletInputStream sis = inStreams.get(0);
		InputStream is = sis.getStream();
		HashMap<String, String> metadata = sis.getMetadata();

		StorletObjectOutputStream sos = (StorletObjectOutputStream)outStreams.get(0);
		OutputStream os = sos.getStream();
		sos.setMetadata(metadata);
		
		//Create a Java stream to process lines from an object.
		//Lines are split by the DELIMITER character.
		Scanner scanner = new Scanner(is, CHARSET).useDelimiter(DELIMITER);
		Stream<String> storletStream = asStream(scanner);	
		
		//list of functions to apply to each record
		//TODO: We need a way of defining the types of functions to instantiate, as we could manipulate String, Ints,...
        List<Function<Stream<String>, Stream<String>>> pushdownFunctions = new ArrayList<>();
        
        //Sort the keys in the parameter map according to the desired order
        List<String> sortedMapKeys = new ArrayList<String>();
        sortedMapKeys.addAll(parameters.keySet());
        Collections.sort(sortedMapKeys);
        
        //Iterate over the parameters that describe the functions to the applied to the stream,
        //compile and instantiate the appropriate lambdas, and add them to the list.
        for (String functionKey: sortedMapKeys){
        	System.out.println(functionKey);
        	//We get a map to instantiate
			if (functionKey.contains("map")){				
				Function<String, String> mapFunction = lambdaFactory.createLambdaUnchecked(
						parameters.get(functionKey), new TypeReference<Function<String, String>>() {});
				pushdownFunctions.add((stream) -> stream.map(mapFunction));				
			//We get a filter to instantiate
			}else if (functionKey.contains("filter")){				
				Predicate<String> filterPredicate = lambdaFactory.createLambdaUnchecked(
						parameters.get(functionKey), new TypeReference<Predicate<String>>() {});
				pushdownFunctions.add((stream) -> stream.filter(filterPredicate));
			}else System.err.println("Warning! Bad function pushdown headers!");			
        }     
        
        //Concatenate all the functions to be applied to a stream.
        Function<Stream<String>, Stream<String>> allPushdownFunctions = pushdownFunctions.stream()
        		.reduce(c -> c, (c1, c2) -> (s -> c2.apply(c1.apply(s))));
        
        //Apply all the functions on each stream record
    	allPushdownFunctions.apply(storletStream)
    		.forEach(t ->  {
        		try {
        			//TODO: Delimiter is needed in the Spark side??
        			os.write((t+DELIMITER).getBytes(), 0, (t+DELIMITER).length());
        		} catch (IOException e) {
        			e.printStackTrace();
        		}});
        	
        //Close streams
        try {	
	        scanner.close();
	        is.close();
			os.close();
        } catch (IOException e) {
			e.printStackTrace();
		}
        
        long after = System.nanoTime();
		logger.emitLog("Stream Record Storlet -- Elapsed [ms]: "+((after-before)/1000000L));
		
	}
}