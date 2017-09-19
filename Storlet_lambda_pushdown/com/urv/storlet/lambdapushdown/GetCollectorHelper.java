package com.urv.storlet.lambdapushdown;

import java.util.Map;
import java.util.AbstractMap.SimpleEntry;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class GetCollectorHelper {
	
	private static final String COMPILED_JOB_PATH = "test.java.storlet";
	
	@SuppressWarnings("rawtypes")
	public static Collector getCollectorObject(String collectorSignature, String collectorType) {
		
		 String className = "Collector"+String.valueOf(Math.abs(collectorSignature.hashCode()));
		 String javaCode = "package " + COMPILED_JOB_PATH + ";\n" +
				 			"import java.util.stream.Collectors; \n" +
				 			"import java.util.stream.Collector; \n" +
				 			"import java.util.AbstractMap.SimpleEntry; \n" +
				 			"import java.util.Map; \n" +
				 			"import " + COMPILED_JOB_PATH + ".IGetCollector; \n" +
				 			
		                    "public class " + className + " implements IGetCollector {\n" +
		                    "    public " + collectorType +" getCollector() {\n" +
		                    "        return (" + collectorType + ")" + collectorSignature + ";\n" +
		                    "    }\n" +
		                    "}\n";
		 
		System.out.println(javaCode);
		long iniTime = System.currentTimeMillis();	
		IGetCollector getCollector = (IGetCollector) CompilationHelper.compileFromString(COMPILED_JOB_PATH, className, javaCode);
		System.out.println("NANO TIME COMPILATION COLLECTOR: " + (System.currentTimeMillis()-iniTime));
		return getCollector.getCollector();
	}
	
	
	/**
	 * Initialize widely used collectors so we can minimize compilation overhead.
	 * 
	 * @param collectorCacheBuilder
	 */
	public void initializeCollectorCache(Map<String, Collector> collectorCacheBuilder) {
		collectorCacheBuilder.put("collect(java.util.stream.Collectors.toList())", Collectors.toList());
		collectorCacheBuilder.put("collect(java.util.stream.Collectors.toSet())", Collectors.toSet());
		collectorCacheBuilder.put("collect(java.util.stream.Collectors.maxBy(String::compareTo))", Collectors.maxBy(String::compareTo));
		collectorCacheBuilder.put("collect(java.util.stream.Collectors.maxBy(Integer::compareTo))", Collectors.maxBy(Integer::compareTo));
		collectorCacheBuilder.put("collect(java.util.stream.Collectors.maxBy(Long::compareTo))", Collectors.maxBy(Long::compareTo));
		collectorCacheBuilder.put("collect(java.util.stream.Collectors.minBy(String::compareTo))", Collectors.minBy(String::compareTo));
		collectorCacheBuilder.put("collect(java.util.stream.Collectors.minBy(Integer::compareTo))", Collectors.minBy(Integer::compareTo));
		collectorCacheBuilder.put("collect(java.util.stream.Collectors.minBy(Long::compareTo))", Collectors.minBy(Long::compareTo));		
		collectorCacheBuilder.put("collect(java.util.stream.Collectors.groupingBy(SimpleEntry<String, Long>::getKey, "
				+ "java.util.stream.Collectors.counting()))", 
				Collectors.groupingBy(SimpleEntry<String, Long>::getKey, Collectors.counting()));
		collectorCacheBuilder.put("collect(java.util.stream.Collectors.groupingBy(SimpleEntry<String, Integer>::getKey, "
				+ "java.util.stream.Collectors.counting()))", 
				Collectors.groupingBy(SimpleEntry<String, Integer>::getKey, Collectors.counting()));
		collectorCacheBuilder.put("collect(java.util.stream.Collectors.counting())", Collectors.counting());
	}

}
