package com.urv.storlet.lambdapushdown;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import main.java.pl.joegreen.lambdaFromString.LambdaFactory;
import main.java.pl.joegreen.lambdaFromString.TypeReference;

/**
 * 
 * This Storlet is intended to dynamically execute code piggybacked into
 * HTTP headers on a data stream from a Swift object request. The idea
 * is to enable a more generic form of delegating computations to the
 * object store to improve the data ingestion problem of Big Data analytics.
 * 
 * This class performs two important tasks:
 * 
 * 1.- It enables to apply a list of functions to each Stream record, as well
 * as to define the order in which such functions will be applied.
 * 
 * 2.- It enables to dynamically compile the functions to be applied on records
 * as defined in the HTTP headers (based on lambdaFromString library).
 * 
 * Such functionality can enable frameworks like Spark to intelligently delegate
 * computations, such as data filtering and transformations, to the Swift cluster,
 * making the analytics jobs much faster. This storlet relies on the LambdaStreamsStorlet
 * to do the byte-level in/out streams conversion to Java 8 Streams.
 * 
 * @author Raul Gracia
 *
 */

public class LambdaPushdownStorlet extends LambdaStreamsStorlet {
	
	//FIXME: To initialize the compiler, the library uses a dummy text file 
	//(helperClassTemplate.txt) that should at the moment exist to avoid errors
	protected LambdaFactory lambdaFactory = LambdaFactory.get();
	
	//This map stores the signature of a lambda as a key and the lambda object as a value.
	//It acts as a cache of repeated lambdas to avoid compilation overhead of already compiled lambdas.
	protected Map<String, Function<Stream<String>, Stream<String>>> lambdaCache = new HashMap<>();	
	
	Pattern lambdaBodyExtraction = Pattern.compile("(map|filter)\\s*?\\(");
	
	@Override
	protected Stream<String> writeYourLambdas(Stream<String> stream) {
		//list of functions to apply to each record
		//TODO: We need a way of defining the types of functions to instantiate, as we could manipulate String, Ints,...?
        List<Function<Stream<String>, Stream<String>>> pushdownFunctions = new ArrayList<>();
        
        //Sort the keys in the parameter map according to the desired order
        List<String> sortedMapKeys = new ArrayList<String>();
        sortedMapKeys.addAll(parameters.keySet());
        Collections.sort(sortedMapKeys);
        
        //Iterate over the parameters that describe the functions to the applied to the stream,
        //compile and instantiate the appropriate lambdas, and add them to the list.
        for (String functionKey: sortedMapKeys){	
        	//If there are no lambdas, continue
        	if ((!functionKey.matches("\\d-lambda"))) continue;
        	
        	String lambdaSignature = parameters.get(functionKey);
        	System.err.println("**>>New lambda to pushdown: " + lambdaSignature);
			if (!lambdaCache.containsKey(lambdaSignature)){
				//We get a map to instantiate
				if (lambdaSignature.startsWith("map")){
					Function<String, String> mapFunction = lambdaFactory.createLambdaUnchecked(
							getLambdaBody(parameters.get(functionKey)), 
								new TypeReference<Function<String, String>>() {});
					lambdaCache.put(lambdaSignature, (s) -> s.map(mapFunction));
					System.err.println("Adding MAP function" + functionKey + " " + mapFunction);
				//We get a filter to instantiate
				}else if (functionKey.contains("filter")){				
					Predicate<String> filterPredicate = lambdaFactory.createLambdaUnchecked(
							getLambdaBody(parameters.get(functionKey)), 
								new TypeReference<Predicate<String>>() {});
					lambdaCache.put(lambdaSignature, (s) -> s.filter(filterPredicate));
					System.err.println("Adding FILTER function" + functionKey + " " + filterPredicate);
				}else System.err.println("Warning! Bad function pushdown headers!");	
			}
			//Add the new compiled function to the list of functions to apply to the stream
			pushdownFunctions.add(lambdaCache.get(lambdaSignature));
        }
        System.err.println("Number of lambdas to execute: " + pushdownFunctions.size());
        
        //Concatenate all the functions to be applied to a stream.
        Function<Stream<String>, Stream<String>> allPushdownFunctions = pushdownFunctions.stream()
        		.reduce(c -> c, (c1, c2) -> (s -> c2.apply(c1.apply(s))));
        
        //Apply all the functions on each stream record
    	return allPushdownFunctions.apply(stream);
	}	
	
	private String getLambdaBody(String lambdaDefinition) {
		Matcher matcher = lambdaBodyExtraction.matcher(lambdaDefinition);
		if(!matcher.find())  
			System.err.println("No match looking for lambda body!");
		return lambdaDefinition.substring(matcher.end(), lambdaDefinition.lastIndexOf(")"));
	}
}