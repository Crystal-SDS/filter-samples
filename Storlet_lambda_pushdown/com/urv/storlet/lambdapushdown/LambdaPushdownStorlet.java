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
import com.urv.storlet.lambdastreams.LambdaStreamsStorlet;

import main.java.pl.joegreen.lambdaFromString.LambdaFactory;
import main.java.pl.joegreen.lambdaFromString.TypeReference;

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

public class LambdaPushdownStorlet extends LambdaStreamsStorlet {
	
	protected static String DELIMITER = "\n";
	protected static String CHARSET = "UTF-8";
	
	//FIXME: To initialize the compiler, the library uses a dummy text file 
	//(helperClassTemplate.txt) that should at the moment exist to avoid errors
	protected LambdaFactory lambdaFactory = LambdaFactory.get();
	
	protected Map<String, Function<Stream<String>, Stream<String>>> lambdaCache = new HashMap<>();
	
	
	@Override
	protected Stream<String> writeYourLambdas(Stream<String> stream) {
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
			if (!lambdaCache.containsKey(parameters.get(functionKey))){
				//We get a map to instantiate
				if (functionKey.contains("map")){
					Function<String, String> mapFunction = lambdaFactory.createLambdaUnchecked(
							parameters.get(functionKey), new TypeReference<Function<String, String>>() {});
					lambdaCache.put(parameters.get(functionKey), (s) -> s.map(mapFunction));
				//We get a filter to instantiate
				}else if (functionKey.contains("filter")){				
					Predicate<String> filterPredicate = lambdaFactory.createLambdaUnchecked(
							parameters.get(functionKey), new TypeReference<Predicate<String>>() {});
					lambdaCache.put(parameters.get(functionKey), (s) -> s.filter(filterPredicate));
				}else System.err.println("Warning! Bad function pushdown headers!");	
			}
			//Add the new compiled function to the list of functions to apply to the stream
			pushdownFunctions.add(lambdaCache.get(parameters.get(functionKey)));
        }
        
        //Concatenate all the functions to be applied to a stream.
        Function<Stream<String>, Stream<String>> allPushdownFunctions = pushdownFunctions.stream()
        		.reduce(c -> c, (c1, c2) -> (s -> c2.apply(c1.apply(s))));
        
        //Apply all the functions on each stream record
    	return allPushdownFunctions.apply(stream);
	}	
}