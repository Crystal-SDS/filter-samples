package com.urv.storlet.lambdapushdown;

import java.util.stream.Collector;

public interface IGetCollector {
	
	@SuppressWarnings("rawtypes")
	public Collector getCollector();

}
