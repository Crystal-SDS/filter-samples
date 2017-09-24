package com.urv.storlet.lambdapushdown;

import net.openhft.compiler.CachedCompiler;
import net.openhft.compiler.CompilerUtils;

public class CompilationHelper {

	private static CachedCompiler compiler = CompilerUtils.CACHED_COMPILER;
	
	public static Object compileFromString(String classPath, String className, String javaCode) {
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
	
	
}
