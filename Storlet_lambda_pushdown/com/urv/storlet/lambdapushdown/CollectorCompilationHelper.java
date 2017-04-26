package com.urv.storlet.lambdapushdown;

import java.util.Map;
import java.util.AbstractMap.SimpleEntry;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import net.openhft.compiler.CachedCompiler;
import net.openhft.compiler.CompilerUtils;

public class CollectorCompilationHelper {
	
	private static final String COMPILED_JOB_PATH = "test.java.storlet";

	private static CachedCompiler compiler = CompilerUtils.CACHED_COMPILER;
	
	@SuppressWarnings("rawtypes")
	public static Collector getCollectorObject(String collectorSignature, String collectorType) {
		
		 String className = "Collector"+String.valueOf(Math.abs(collectorSignature.hashCode()));
		 String javaCode = "package " + COMPILED_JOB_PATH + ";\n" +
				 			"import java.util.stream.Collectors; \n" +
				 			"import java.util.stream.Collector; \n" +
				 			"import java.util.AbstractMap.SimpleEntry; \n" +
				 			"import java.util.Map; \n" +
				 			"import test.java.storlet.IGetCollector; \n" +
				 			
		                    "public class " + className + " implements IGetCollector {\n" +
		                    "    public " + collectorType +" getCollector() {\n" +
		                    "        return (" + collectorType + ")" + collectorSignature + ";\n" +
		                    "    }\n" +
		                    "}\n";
		 
		System.out.println(javaCode);
		long iniTime = System.currentTimeMillis();	
		IGetCollector getCollector = (IGetCollector) compileFromString(COMPILED_JOB_PATH, className, javaCode);
		System.out.println("NANO TIME COMPILATION COLLECTOR: " + (System.currentTimeMillis()-iniTime));
		return getCollector.getCollector();
	}
	
	private static  Object compileFromString(String classPath, String className, String javaCode) {
		try {
			javaCode = javaCode.replace(className, className+"_");
			Class aClass = compiler.loadFromJava(classPath+"."+className+"_", javaCode);
			Object obj = aClass.newInstance();
			return obj;
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	 }
	
	/**
	 * Initialize widely used collectors so we can minimize compilation overhead.
	 * 
	 * @param collectorCacheBuilder
	 */
	void initializeCollectorCache(Map<String, Collector> collectorCacheBuilder) {
		collectorCacheBuilder.put("collect(Collectors.toList())", Collectors.toList());
		collectorCacheBuilder.put("collect(Collectors.toSet())", Collectors.toSet());
		collectorCacheBuilder.put("collect(Collectors.maxBy(String::compareTo))", Collectors.maxBy(String::compareTo));
		collectorCacheBuilder.put("collect(Collectors.maxBy(Integer::compareTo))", Collectors.maxBy(Integer::compareTo));
		collectorCacheBuilder.put("collect(Collectors.maxBy(Long::compareTo))", Collectors.maxBy(Long::compareTo));
		collectorCacheBuilder.put("collect(Collectors.minBy(String::compareTo))", Collectors.minBy(String::compareTo));
		collectorCacheBuilder.put("collect(Collectors.minBy(Integer::compareTo))", Collectors.minBy(Integer::compareTo));
		collectorCacheBuilder.put("collect(Collectors.minBy(Long::compareTo))", Collectors.minBy(Long::compareTo));		
		collectorCacheBuilder.put("collect(Collectors.groupingBy(SimpleEntry<String, Long>::getKey, Collectors.counting()))", 
				Collectors.groupingBy(SimpleEntry<String, Long>::getKey, Collectors.counting()));
		collectorCacheBuilder.put("collect(Collectors.groupingBy(SimpleEntry<String, Integer>::getKey, Collectors.counting())", 
				Collectors.groupingBy(SimpleEntry<String, Integer>::getKey, Collectors.counting()));
		collectorCacheBuilder.put("collect(Collectors.counting())", Collectors.counting());
	}
}
