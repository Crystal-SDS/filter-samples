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
import java.lang.reflect.Parameter;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Stream;

import com.ibm.storlet.common.IStorlet;
import com.ibm.storlet.common.StorletException;
import com.ibm.storlet.common.StorletInputStream;
import com.ibm.storlet.common.StorletLogger;
import com.ibm.storlet.common.StorletObjectOutputStream;
import com.ibm.storlet.common.StorletOutputStream;

import pl.joegreen.lambdaFromString.LambdaFactory;
import pl.joegreen.lambdaFromString.LambdaFactoryConfiguration;
import pl.joegreen.lambdaFromString.TypeReference;

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
 * making the analytics jobs much faster. This storlet does the byte-level in/out 
 * streams conversion into Java 8 Streams.
 * 
 * @author Raul Gracia
 *
 */

public class LambdaPushdownStorlet implements IStorlet {
	
	protected final Charset CHARSET = Charset.forName("UTF-8");
	protected final int BUFFER_SIZE = 128*1024;
	
	protected Map<String, String> parameters = null;
	private StorletLogger logger;
	
	//Classes that can be used within lambdas for compilation
	protected LambdaFactory lambdaFactory = LambdaFactory.get(LambdaFactoryConfiguration.get()
			.withImports(BigDecimal.class, Arrays.class, Set.class, Map.class, SimpleEntry.class, 
					Date.class, Instant.class, SimpleDateFormat.class, DateTimeFormatter.class,
						ArrayList.class, HashSet.class, HashMap.class));		
	
	//This map stores the signature of a lambda as a key and the lambda object as a value.
	//It acts as a cache of repeated lambdas to avoid compilation overhead of already compiled lambdas.
	protected Map<String, Function> lambdaCache = new HashMap<>();
	//protected Map<String, Collector> collectorCache = new HashMap<>();
	protected Map<String, Function> reducerCache = new HashMap<>();
	
	private Pattern lambdaBodyExtraction = Pattern.compile("(map|filter|flatMap|collect|reduce)\\s*?\\(");
	
	private static final String LAMBDA_TYPE_AND_BODY_SEPARATOR = "|";
	//There is a problem using "," when passing lambdas as Storlet parameters, as the
	//Storlet middleware treats every "," as a separation between key/value parameter pairs
	private static final String COMMA_REPLACEMENT_IN_PARAMS = "'";
	private static final String EQUAL_REPLACEMENT_IN_PARAMS = "$";
	private static final String ADD_FILE_HEADER = "add_header";
	private static final String SEQUENTIAL_STREAM = "sequential";
	
	private static final String noneType = "None<>";
	
	public LambdaPushdownStorlet() {
		GetCollectorHelper.initializeCollectorCache();
		GetTypeReferenceHelper.initializeTypeReferenceCache();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected Stream writeYourLambdas(Stream<String> stream) {
		//list of functions to apply to each record
        List<Function<Stream, Stream>> pushdownFunctions = new ArrayList<>();
        Collector pushdownCollector = null;
        Function pushdownReducer = null;
        boolean hasTerminalLambda = false;
        
        //Sort the keys in the parameter map according to the desired order
        List<String> sortedMapKeys = new ArrayList<String>();
        sortedMapKeys.addAll(parameters.keySet());
        Collections.sort(sortedMapKeys);        

		long iniCompileTime = System.currentTimeMillis();
        //Iterate over the parameters that describe the functions to the applied to the stream,
        //compile and instantiate the appropriate lambdas, and add them to the list.
        for (String functionKey: sortedMapKeys){	
        	//If this keys is not related with lambdas, just continue
        	if ((!functionKey.matches("\\d-lambda"))) continue;
        	
        	//Get the signature of the function to compile
        	String lambdaTypeAndBody = parameters.get(functionKey).replace(COMMA_REPLACEMENT_IN_PARAMS, ",")
        														  .replace(EQUAL_REPLACEMENT_IN_PARAMS, "=");
        	int separatorPos = lambdaTypeAndBody.indexOf(LAMBDA_TYPE_AND_BODY_SEPARATOR);
        	String lambdaType = lambdaTypeAndBody.substring(0, separatorPos);
        	String lambdaBody = lambdaTypeAndBody.substring(separatorPos+1);
        	
        	//Check if we have already compiled this lambda and exists in the cache			
			//Compile the lambda and add it to the cache
			if (lambdaBody.startsWith("collect")){
				pushdownCollector = getCollectorObject(lambdaBody, lambdaType);
		        hasTerminalLambda = true;
			}else if (lambdaBody.startsWith("reduce")){
				pushdownReducer = getAndCacheCompiledFunction(lambdaBody, lambdaType, reducerCache);
		        hasTerminalLambda = true;
			} else { 
				//Add the new compiled function to the list of functions to apply to the stream
				pushdownFunctions.add(getAndCacheCompiledFunction(lambdaBody, lambdaType, lambdaCache));
			}
        }
        logger.emitLog("Number of lambdas to execute: " + pushdownFunctions.size());
        
        //Avoid overhead of composing functions
        if (pushdownFunctions.size()==0 && !hasTerminalLambda) return stream;
        
        //Concatenate all the functions to be applied to a data stream
        Function allPushdownFunctions = pushdownFunctions.stream()
        		.reduce(c -> c, (c1, c2) -> (s -> c2.apply(c1.apply(s))));   
        Stream<Object> potentialTerminals = Arrays.asList(pushdownCollector, pushdownReducer).stream();
        logger.emitLog("Compilation time: " + (System.currentTimeMillis()-iniCompileTime) + "ms");
        System.out.println("Compilation time: " + (System.currentTimeMillis()-iniCompileTime) + "ms");
        
        //Apply all the functions on each stream record
    	return hasTerminalLambda ? applyTerminalOperation((Stream) allPushdownFunctions.apply(stream), 
    			potentialTerminals.filter(f -> f!=null).findFirst().get()): 
    				(Stream) allPushdownFunctions.apply(stream);
	}	
	
	@Override
	public void invoke(ArrayList<StorletInputStream> inStreams,	ArrayList<StorletOutputStream> outStreams, 
			Map<String, String> parameters, StorletLogger logger) throws StorletException {
			
		long before = System.nanoTime();
		logger.emitLog("----- Init " + this.getClass().getName() + " -----");
		
		//Get streams and parameters
		StorletInputStream sis = inStreams.get(0);
		InputStream is = sis.getStream();
		HashMap<String, String> metadata = sis.getMetadata();
		StorletObjectOutputStream sos = (StorletObjectOutputStream) outStreams.get(0);
		OutputStream os = sos.getStream();
		sos.setMetadata(metadata);
		
		this.parameters = parameters;
		this.logger = logger;
		
		//To improve performance, we have a bimodal way of writing streams. If we have lambdas,
		//execute those lambdas on BufferedWriter/Readers, as we need to operate on text and
		//do the encoding from bytes to strings. If there are no lambdas, we can directly manage
		//byte streams, having much better throughput.
		if (requestContainsLambdas(parameters)){
			applyLambdasOnDataStream(is, os);
		} else writeByteBasedStreams(is, os); 
		
        this.logger = null;
        long after = System.nanoTime();
		logger.emitLog(this.getClass().getName() + " -- Elapsed [ms]: "+((after-before)/1000000L));			
	}
	
	@SuppressWarnings("unchecked")
	protected void applyLambdasOnDataStream(InputStream is, OutputStream os) {
		Long inputBytes = 0L;
		long iniTime = System.nanoTime();
	
		//Convert InputStream as a Stream, and apply lambdas
		BufferedWriter writeBuffer = new BufferedWriter(new OutputStreamWriter(os, CHARSET), BUFFER_SIZE);
		BufferedReader readBuffer = new BufferedReader(new InputStreamReader(is, CHARSET), BUFFER_SIZE); 
		//Check if we have to write the first line of the file always
		if (parameters.containsKey(ADD_FILE_HEADER)){
			try {
				writeBuffer.write(readBuffer.readLine());
				writeBuffer.newLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		//By default the streams are parallel, but if ordering is necessary then convert it to sequential
		Stream<String> dataStream = readBuffer.lines().parallel();
		if (parameters.containsKey(SEQUENTIAL_STREAM))
			dataStream = dataStream.sequential();
					
		//Create an iterator with the results of the computations of the lambdas
		Iterator<String> resultsIterator = ((Stream<String>) writeYourLambdas(dataStream)
												.map(s -> formatOutput(s)))
												.iterator();
		//Write the results in a thread-safe manner	
		try {
			while (resultsIterator.hasNext()) {
				String lineString = resultsIterator.next();
				writeBuffer.write(lineString);
				inputBytes+=lineString.length();
				if (resultsIterator.hasNext()) 
					writeBuffer.newLine();
			}
			logger.emitLog("Closing the streams after lambda execution...");
			writeBuffer.close();
			is.close();
			os.flush();
			os.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.emitLog("STREAMS BW: " + ((inputBytes/1024./1024.) + " MB /" +
			((System.nanoTime()-iniTime)/1000000000.)) + " secs = " + ((inputBytes/1024./1024.)/
					((System.nanoTime()-iniTime)/1000000000.)) + " MBps");
	}
	
	/**
	 * Make sure that the output of lambdas is in an understandable format for a
	 * client. This is mainly needed to write the output of collections correctly.
	 *  
	 * @param line
	 * @return formatted output
	 */
	private static String formatOutput(Object line) {
		String lineString = "";
		if (line instanceof List){
			StringBuilder sb = new StringBuilder();
			String prefix = "";
			for (Object o: (List) line) {
				sb.append(prefix).append(o.toString());
				prefix = ",";
			}
			lineString = sb.toString();
		//As we handle different types of object, invoke toString
		}else lineString = line.toString();						
		return lineString;
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
	
	@SuppressWarnings("rawtypes")
	private Stream applyTerminalOperation(Stream functionsOnStream, Function terminalOperation) {	
		//Executing a terminal operation forces us to consume the stream, which may be bad for performance
		try {
			return Stream.of(((Optional) terminalOperation.apply(functionsOnStream)).get());
		} catch (NoSuchElementException e) {
			System.err.println("Terminal operation without result in Optional value.");
		}
		//Temporal default value
		return Stream.of("");		
	}
	
	@SuppressWarnings("rawtypes")
	private Stream applyTerminalOperation(Stream functionsOnStream, Object terminalOperation) {
		if (terminalOperation instanceof Function) 
			return applyTerminalOperation(functionsOnStream, (Function) terminalOperation);
		if (terminalOperation instanceof Collector)
			return applyTerminalOperation(functionsOnStream, (Collector) terminalOperation);
		//Temporal default value
		return Stream.of("");
	}

	private void writeByteBasedStreams(InputStream is, OutputStream os) {
		byte[] buffer = new byte[BUFFER_SIZE];
		int len, inputBytes = 0;	
		long iniTime = System.nanoTime();
		try {				
			while((len=is.read(buffer)) != -1) {
				os.write(buffer, 0, len);
				inputBytes+=len;
			}
			is.close();
			os.close();
		} catch (IOException e) {
			logger.emitLog(this.getClass().getName() + " raised IOException: " + e.getMessage());
		}		
		logger.emitLog("NOOP BW: " + ((inputBytes/1024./1024.)/((System.nanoTime()-iniTime)/1000000000.)) + "MBps");
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
			Method theMethod = getMethodInvocation(methodName, lambdaType);			
			function = (s) -> {						
				try {
					System.out.println("Goingto execute: " + lambdaSignature + " " + theMethod.getParameterCount());
					return (lambdaType.equals(noneType))? invokeMethodOnStream((Stream) s, theMethod, lambdaSignature):
						theMethod.invoke(((Stream) s), lambdaFactory.createLambdaUnchecked(
						getLambdaBody(lambdaSignature), getLambdaType(methodName, lambdaType)));
				} catch (IllegalAccessException|InvocationTargetException e) {
					System.err.println("Error invoking a pushdown method on the Stream class: " + lambdaSignature);
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
	
	private Function getAndCacheCompiledFunction(String lambdaSignature, String lambdaType, Map<String, Function> cache){
		if (cache.containsKey(lambdaSignature)) 
			return cache.get(lambdaSignature);
		Function lambda = getFunctionObject(lambdaSignature, lambdaType);
		cache.put(lambdaSignature, lambda);
		return lambda;
	}
	
	private Object invokeMethodOnStream(Stream s, Method theMethod, String lambdaSignature) 
						throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		//For methods without params, just invoke it (like distinct)
		if (theMethod.getParameterTypes().length==0) 
			return theMethod.invoke(((Stream) s));
		
		//For methods with params, we have to infer them for the invocation (like skip(n) or limit (n))
		Parameter[] methodParams = theMethod.getParameters();
		String[] parameterSignature = lambdaSignature.substring(lambdaSignature.indexOf("(")+1,
													lambdaSignature.lastIndexOf(")")).split(",");
		Object[] convertedParams = new Object[methodParams.length];
		int index = 0;
		for (Parameter p: methodParams) {
			if (p.getParameterizedType().toString().startsWith("long") || p.getParameterizedType().toString().startsWith("java.lang.Long")) 
					convertedParams[index] = Long.valueOf(parameterSignature[index]);
			else if (p.getParameterizedType().toString().startsWith("int") || p.getParameterizedType().toString().startsWith("java.lang.Integer")) 
				convertedParams[index] = Long.valueOf(parameterSignature[index]);
			else if (p.getParameterizedType().toString().startsWith("double") || p.getParameterizedType().toString().startsWith("java.lang.Double")) 
				convertedParams[index] = Long.valueOf(parameterSignature[index]);
			else if (p.getParameterizedType().toString().startsWith("boolean") || p.getParameterizedType().toString().startsWith("java.lang.Boolean")) 
				convertedParams[index] = Long.valueOf(parameterSignature[index]);
			else if (p.getParameterizedType().toString().startsWith("java.lang.String")) 
				convertedParams[index] = Long.valueOf(parameterSignature[index]);
			index++;
		}
		//This is mainly used for simple methods requiring few simple arguments, like skip or limit
		switch (methodParams.length) {
			case 1:	return theMethod.invoke(((Stream) s), convertedParams[0]);
			case 2:	return theMethod.invoke(((Stream) s), convertedParams[0], convertedParams[1]);
			default: System.err.println("Trying to invoke a non-lambda method with more than 2 parameters. "
					 + "We do not consider these methods, so this will crash.");
				break;
		};		
		return null;
	}
	
	private Method getMethodInvocation(String methodName, String lambdaType) 
			throws NoSuchMethodException, SecurityException, ClassNotFoundException {
		Method theMethod = null;
		if (lambdaType.equals(noneType)) {
			theMethod = Stream.of(Stream.class.getMethods())
					.filter(m -> m.getName().equals(methodName))
					.findFirst().get();
		}else theMethod = Stream.class.getMethod(methodName, Class.forName(
					lambdaType.substring(0, lambdaType.indexOf("<")))); 
		return theMethod;
	}

	@SuppressWarnings({"rawtypes"})
	private Collector getCollectorObject(String lambdaSignature, String lambdaType) {
		Collector function = null;
		try {
			return GetCollectorHelper.getCollectorObject(getLambdaBody(lambdaSignature), lambdaType);
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
			Method theMethod = GetTypeReferenceHelper.class.getMethod(supportedTypesMethod, String.class);
			return (TypeReference) theMethod.invoke(GetTypeReferenceHelper.class, lambdaType);
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | 
				IllegalArgumentException | InvocationTargetException e) {
			e.printStackTrace();
		}
		return null;		
	}
}