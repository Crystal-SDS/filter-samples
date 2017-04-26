package com.urv.storlet.lambdapushdown;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Stream;

import org.openstack.storlet.common.StorletException;
import org.openstack.storlet.common.StorletInputStream;
import org.openstack.storlet.common.StorletLogger;
import org.openstack.storlet.common.StorletObjectOutputStream;
import org.openstack.storlet.common.StorletOutputStream;

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
	
	protected LambdaFactory lambdaFactory = LambdaFactory.get();
	
	//This map stores the signature of a lambda as a key and the lambda object as a value.
	//It acts as a cache of repeated lambdas to avoid compilation overhead of already compiled lambdas.
	protected Map<String, Function> lambdaCache = new HashMap<>();
	protected Map<String, Collector> collectorCache = new HashMap<>();
	
	private Pattern lambdaBodyExtraction = Pattern.compile("(map|filter|flatMap|collect)\\s*?\\(");
	//private Pattern intermediateLambdas = Pattern.compile("(map|filter|flatMap|collect|mapToPair|reduceByKey)");
	//private Pattern terminalLambdas = Pattern.compile("(collect|count)");
	
	private static final String LAMBDA_TYPE_AND_BODY_SEPARATOR = "|";
	
	public LambdaPushdownStorlet() {
		new CollectorCompilationHelper().initializeCollectorCache(collectorCache);
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected Stream writeYourLambdas(Stream<String> stream) {
		long initime = System.currentTimeMillis();
		//list of functions to apply to each record
        List<Function<Stream, Stream>> pushdownFunctions = new ArrayList<>();
        Collector pushdownCollector = null;
        boolean hasTerminalLambda = false;
        
        //Sort the keys in the parameter map according to the desired order
        List<String> sortedMapKeys = new ArrayList<String>();
        sortedMapKeys.addAll(parameters.keySet());
        Collections.sort(sortedMapKeys);
        
        //Iterate over the parameters that describe the functions to the applied to the stream,
        //compile and instantiate the appropriate lambdas, and add them to the list.
        for (String functionKey: sortedMapKeys){	
        	//If this keys is not related with lambdas, just continue
        	if ((!functionKey.matches("\\d-lambda"))) continue;
        	
        	//Get the signature of the function to compile
        	String lambdaTypeAndBody = parameters.get(functionKey);
        	String lambdaType = lambdaTypeAndBody.substring(0, 
        			lambdaTypeAndBody.indexOf(LAMBDA_TYPE_AND_BODY_SEPARATOR));
        	String lambdaBody = lambdaTypeAndBody.substring(
        			lambdaTypeAndBody.indexOf(LAMBDA_TYPE_AND_BODY_SEPARATOR)+1);
        	System.out.println(lambdaTypeAndBody);
        	System.out.println("**>>New lambda to pushdown: " + lambdaType + " ->>> " + lambdaBody);
        	
        	//Check if we have already compiled this lambda and exists in the cache
			if (lambdaCache.containsKey(lambdaBody)) {
				pushdownFunctions.add(lambdaCache.get(lambdaBody));
				continue;
			}else if (collectorCache.containsKey(lambdaBody)){
				pushdownCollector = collectorCache.get(lambdaBody);
				hasTerminalLambda = true;
				continue;
			}
			
			//Compile the lambda and add it to the cache
			if (lambdaBody.startsWith("collect")){
				pushdownCollector = getCollectorObject(lambdaBody, lambdaType);
				collectorCache.put(lambdaBody, pushdownCollector);
		        hasTerminalLambda = true;
			} else { 
				//Add the new compiled function to the list of functions to apply to the stream
				lambdaCache.put(lambdaBody, getFunctionObject(lambdaBody, lambdaType));			
				pushdownFunctions.add(lambdaCache.get(lambdaBody));
			}
        }
        System.out.println("Number of lambdas to execute: " + pushdownFunctions.size());
        
        //Concatenate all the functions to be applied to a data stream
        Function allPushdownFunctions = pushdownFunctions.stream()
        		.reduce(c -> c, (c1, c2) -> (s -> c2.apply(c1.apply(s))));        

        System.out.println("Compilation time: " + (System.currentTimeMillis()-initime) + "ms");
        
        //Apply all the functions on each stream record
    	return hasTerminalLambda ? applyTerminalOperation((Stream) allPushdownFunctions.apply(stream), 
    			pushdownCollector):(Stream) allPushdownFunctions.apply(stream);
	}	
	
	@Override
	public void invoke(ArrayList<StorletInputStream> inStreams,	ArrayList<StorletOutputStream> outStreams, 
			Map<String, String> parameters, StorletLogger logger) throws StorletException {
			
		long before = System.nanoTime();
		logger.emitLog("----- Init " + this.getClass().getName() + " -----");
		System.out.println(">>>>>>>>>>>>>>> INVOKE LAMBDA PUSHDOWN STORLET!!!");
		System.out.println(">>>>>>>>>>>>>>> " + parameters.toString());
		System.err.println(">>>>>>>>>>>>>>> INVOKE LAMBDA PUSHDOWN STORLET!!!");
		System.err.println(">>>>>>>>>>>>>>> " + parameters.toString());
		
		//Get streams and parameters
		StorletInputStream sis = inStreams.get(0);
		InputStream is = sis.getStream();
		HashMap<String, String> metadata = sis.getMetadata();
		StorletObjectOutputStream sos = (StorletObjectOutputStream) outStreams.get(0);
		OutputStream os = sos.getStream();
		sos.setMetadata(metadata);
		
		this.parameters = parameters;
		
		//To improve performance, we have a bimodal way of writing streams. If we have lambdas,
		//execute those lambdas on BufferedWriter/Readers, as we need to operate on text and
		//do the encoding from bytes to strings. If there are no lambdas, we can directly manage
		//byte streams, having much better throughput.
		if (requestContainsLambdas(parameters)){
			applyLambdasOnDataStream(is, os, logger);
		} else writeByteBasedStreams(is, os, logger); 
		
        long after = System.nanoTime();
		logger.emitLog(this.getClass().getName() + " -- Elapsed [ms]: "+((after-before)/1000000L));			
	}
	
	@SuppressWarnings("unchecked")
	protected void applyLambdasOnDataStream(InputStream is, OutputStream os, StorletLogger logger) {
		try{
			//Convert InputStream as a Stream, and apply lambdas
			BufferedWriter writeBuffer = new BufferedWriter(new OutputStreamWriter(os, CHARSET), BUFFER_SIZE);
			BufferedReader readBuffer = new BufferedReader(new InputStreamReader(is, CHARSET), BUFFER_SIZE); 
			writeYourLambdas(readBuffer.lines().parallel()).forEach(line -> {	
				try {
					//FIXME: We have to call toString here to write a String from unknown Stream items
					writeBuffer.write(line.toString());  
					writeBuffer.newLine();
				}catch(IOException e){
					logger.emitLog(this.getClass().getName() + " raised IOException: " + e.getMessage());
					e.printStackTrace(System.err);
				}
			});
			writeBuffer.close();
			is.close();
			os.close();
		} catch (IOException e1) {
			logger.emitLog(this.getClass().getName() + " raised IOException 2: " + e1.getMessage());
			e1.printStackTrace(System.err);
		}		
	}
	
	/**
	 * In this method we have to treat differently the supported terminal operations. There are
	 * operations that return collections of objects, normally from collect operations (List, Map)
	 * and other operations that return just a single number (count, sum, min, max). We have to deal
	 * with these cases and return the result as a (non-parallel) stream (otherwise the output of the
	 * processing will show errors related to uncontrolled writes on the output stream).
	 * 
	 * @param writeYourLambdas
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	private Stream applyTerminalOperation(Stream functionsOnStream, Collector terminalOperation) {	
		//Executing a terminal operation forces us to consume the stream, which may be bad for performance
		Object collectorResult = functionsOnStream.collect(terminalOperation);
		if (collectorResult instanceof Map) return ((Map) collectorResult).entrySet().stream();
		if (collectorResult instanceof List) return ((List) collectorResult).stream();
		if (collectorResult instanceof Set) return ((Set) collectorResult).stream();
		if (collectorResult instanceof Optional){
			Optional result = (Optional) collectorResult;
			return result.isPresent()? Stream.of(result.get()): Stream.of("");
		}
		return Stream.of(collectorResult);		
	}

	private void writeByteBasedStreams(InputStream is, OutputStream os, StorletLogger logger) {
		byte[] buffer = new byte[BUFFER_SIZE];
		int len;		
		try {				
			while((len=is.read(buffer)) != -1) {
				os.write(buffer, 0, len);
			}
			is.close();
			os.close();
		} catch (IOException e) {
			logger.emitLog(this.getClass().getName() + " raised IOException: " + e.getMessage());
		}		
	}

	private boolean requestContainsLambdas(Map<String, String> parameters) {
		for (String key: parameters.keySet())
			if (key.contains("-lambda")) return true;
		return false;
	}
	
	private String getLambdaBody(String lambdaDefinition) {
		Matcher matcher = lambdaBodyExtraction.matcher(lambdaDefinition);
		if(!matcher.find())  
			System.err.println("No match looking for lambda body!");
		return lambdaDefinition.substring(matcher.end(), lambdaDefinition.lastIndexOf(")"));
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Function getFunctionObject(String lambdaSignature, String lambdaType) {
		String methodName = lambdaSignature.substring(0, lambdaSignature.indexOf("("));	
		Function function = null;
		try {
			//Get the method to invoke via reflection
			Method theMethod = Stream.class.getMethod(methodName, Class.forName(
					lambdaType.substring(0, lambdaType.indexOf("<"))));
			function = (s) -> {						
				try {
					System.out.println(lambdaSignature);
					return theMethod.invoke(((Stream) s), lambdaFactory.createLambdaUnchecked(
						getLambdaBody(lambdaSignature), getLambdaType(methodName, lambdaType)));
				} catch (IllegalAccessException|InvocationTargetException e) {
					System.err.println("Error invoking a pushdown method on the Stream class.");
					e.printStackTrace();
				}
				return null;		
			};
		} catch (NoSuchMethodException | SecurityException | IllegalArgumentException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}		
		return function;
	}
	
	@SuppressWarnings({"rawtypes"})
	private Collector getCollectorObject(String lambdaSignature, String lambdaType) {
		Collector function = null;
		try {
			return CollectorCompilationHelper.getCollectorObject(getLambdaBody(lambdaSignature), lambdaType);
		} catch (SecurityException | IllegalArgumentException e) {
			e.printStackTrace();
		}		
		return function;
	}
	
	/**
	 * 
	 * 
	 * @param methodName
	 * @param lambdaType
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	private TypeReference getLambdaType(String methodName,  String lambdaType) {
		String supportedTypesMethod = "get" + methodName.substring(0,1).toUpperCase() + 
				methodName.substring(1) + "Type";
		try {
			Method theMethod = SupportedLambdaTypes.class.getMethod(supportedTypesMethod, String.class);
			return (TypeReference) theMethod.invoke(SupportedLambdaTypes.class, lambdaType);
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | 
				IllegalArgumentException | InvocationTargetException e) {
			e.printStackTrace();
		}
		return null;		
	}
}