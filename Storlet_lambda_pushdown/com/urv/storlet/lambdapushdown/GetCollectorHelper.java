package com.urv.storlet.lambdapushdown;

import java.util.HashMap;
import java.util.Map;
import java.util.AbstractMap.SimpleEntry;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class GetCollectorHelper {
	
	private static final String COMPILED_JOB_PATH = "test.java.storlet";

	private static Map<String, Collector> collectorCache = new HashMap<>();
	
	@SuppressWarnings("unused")
	public static synchronized Collector getCollectorObject(String collectorSignature, String collectorType) {
		Collector result = collectorCache.get(collectorSignature);
		if (result!=null) return result;
		result = compileCollectorObject(collectorSignature, collectorType);
		collectorCache.put(collectorSignature, result);
		return result;
	}
	
	@SuppressWarnings("rawtypes")
	public static Collector compileCollectorObject(String collectorSignature, String collectorType) {
		
		 String className = "Collector"+String.valueOf(Math.abs(collectorSignature.hashCode()));
		 String javaCode = "package " + COMPILED_JOB_PATH + ";\n" +
				 			"import java.util.stream.Collectors; \n" +
				 			"import java.util.stream.Collector; \n" +
				 			"import java.util.AbstractMap.SimpleEntry; \n" +
				 			"import java.util.Map; \n" +
				 			"import com.urv.storlet.lambdapushdown.IGetCollector; \n" +
				 			
		                    "public class " + className + " implements IGetCollector {\n" +
		                    "    public " + collectorType +" getCollector() {\n" +
		                    "        return (" + collectorType + ")" + collectorSignature + ";\n" +
		                    "    }\n" +
		                    "}\n";
		 
		long iniTime = System.currentTimeMillis();	
		IGetCollector getCollector = (IGetCollector) CompilationHelper.compileFromString(COMPILED_JOB_PATH, className, javaCode);
		System.out.println("Collector compilatoin time (ms): " + (System.currentTimeMillis()-iniTime));
		return getCollector.getCollector();
	}
	
	
	/**
	 * Initialize widely used collectors so we can minimize compilation overhead.
	 * 
	 * @param collectorCacheBuilder
	 */
	public static void initializeCollectorCache() {
		collectorCache.put("collect(java.util.stream.Collectors.toList())", Collectors.toList());
		collectorCache.put("collect(java.util.stream.Collectors.toSet())", Collectors.toSet());
		collectorCache.put("collect(java.util.stream.Collectors.maxBy(String::compareTo))", Collectors.maxBy(String::compareTo));
		collectorCache.put("collect(java.util.stream.Collectors.maxBy(Integer::compareTo))", Collectors.maxBy(Integer::compareTo));
		collectorCache.put("collect(java.util.stream.Collectors.maxBy(Long::compareTo))", Collectors.maxBy(Long::compareTo));
		collectorCache.put("collect(java.util.stream.Collectors.minBy(String::compareTo))", Collectors.minBy(String::compareTo));
		collectorCache.put("collect(java.util.stream.Collectors.minBy(Integer::compareTo))", Collectors.minBy(Integer::compareTo));
		collectorCache.put("collect(java.util.stream.Collectors.minBy(Long::compareTo))", Collectors.minBy(Long::compareTo));		
		collectorCache.put("collect(java.util.stream.Collectors.groupingBy(SimpleEntry<String, Long>::getKey, "
				+ "java.util.stream.Collectors.counting()))", 
				Collectors.groupingBy(SimpleEntry<String, Long>::getKey, Collectors.counting()));
		collectorCache.put("collect(java.util.stream.Collectors.groupingBy(SimpleEntry<String, Integer>::getKey, "
				+ "java.util.stream.Collectors.counting()))", 
				Collectors.groupingBy(SimpleEntry<String, Integer>::getKey, Collectors.counting()));
		collectorCache.put("collect(java.util.stream.Collectors.counting())", Collectors.counting());
	}

}
