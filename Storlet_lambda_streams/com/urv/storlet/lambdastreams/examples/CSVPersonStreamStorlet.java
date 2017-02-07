package com.urv.storlet.lambdastreams.examples;

import java.util.function.Function;
import java.util.stream.Stream;

import com.urv.storlet.lambdastreams.LambdaStreamsStorlet;

/**
 * Example of how we can benefit from managing Storlet streams into
 * Java 8 Streams. This example requires a CSV input file such as:
 * 
 * Ola Nordmann, 61, Sandnes, Norway
 * Viswanathan Anand, 43, Mayiladuthurai, India
 * ...
 * 
 * @author Raul Gracia
 *
 */
public class CSVPersonStreamStorlet extends LambdaStreamsStorlet {

	@Override
	protected Stream<String> writeYourLambdas(Stream<String> stream) {
		return stream.map(mapToPerson)
			    .filter(person -> person.getAge() > 17)
			    .map(person -> {
			    	System.out.println(person.toString()); 
			    	return person.toString();
			    });
	}
	
	private Function<String, Person> mapToPerson = (line) -> {
		  String[] p = line.split(", ");
		  return new Person(p[0], Integer.parseInt(p[1]), p[2], p[3]);
	};
	
}

class Person {
	
	String name;
	Integer age;
	String city;
	String country;
	
	public Person(String name, Integer age, String city, String country) {
		this.name = name;
		this.age = age;
		this.city = city;
		this.country = country;
	}
	
	@Override
	public String toString() {
		return name + "," + age + "," + city + "," + country;
	}

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Integer getAge() {
		return age;
	}
	public void setAge(Integer age) {
		this.age = age;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}	
}