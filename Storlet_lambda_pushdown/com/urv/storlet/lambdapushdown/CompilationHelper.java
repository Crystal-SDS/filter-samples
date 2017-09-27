package com.urv.storlet.lambdapushdown;

import net.openhft.compiler.CachedCompiler;
import net.openhft.compiler.CompilerUtils;

public class CompilationHelper {

	private static CachedCompiler compiler = CompilerUtils.CACHED_COMPILER;
	
	private static final String COMPILED_JOB_PATH = "test.java.storlet";
	
	private final static String commonImports = "package " + COMPILED_JOB_PATH + ";\n" +
 			"import java.util.stream.Stream; \n" +
 			"import java.util.stream.Collector; \n" +
 			"import java.util.stream.Collectors; \n" +
 			"import java.util.AbstractMap.SimpleEntry; \n" +
 			"import java.util.List; \n" +
 			"import java.util.ArrayList; \n" +
 			"import java.util.Set; \n" +
 			"import java.util.Map; \n";
	
	public static Object compileFromString(String className, String javaCode) {
		return compileFromString(COMPILED_JOB_PATH, className, javaCode);
	}
	
	public static Object compileFromString(String classPath, String className, String javaCode) {
		try {
			javaCode = commonImports + javaCode;
			javaCode = javaCode.replace(className, className+"_");
			Class aClass = compiler.loadFromJava(classPath+"."+className+"_", javaCode);
			Object obj = aClass.newInstance();
			return obj;
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	 }
}
