package com.stackroute.datamunging.test;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.stackroute.datamunging.Query;
import com.stackroute.datamunging.ResultSet;
import com.stackroute.query.parser.AggregateFunction;

public class IPLTestCase {

	private static Query query;

	private String queryString;

	@BeforeClass
	public static void init() {
	
			query = new Query();
	}
	

	@Test
	public void getAllRecords() {
		queryString = "select city,winner,team1,team2 from ipl.csv";
		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayRecords(queryString, records);
	}
	
	@Test
	public void getCountOfCities() {
		queryString = "select count(city) from ipl.csv";
		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayAggregateResult(queryString, records.getAggregateFunctions());
	}
	
	
	private void displayAggregateResult(String queryString, List<AggregateFunction> aggregateFunctions) {
		System.out.println("\nGiven Query : " + queryString);
		aggregateFunctions.forEach(aggregate->{
			System.out.print(aggregate.getFunction()+"(");
			System.out.print(aggregate.getField()+"): ");
		System.out.println(aggregate.getResult());});		
	}
	private void displayRecords(String queryString, ResultSet records) {
		System.out.println("\nGiven Query : " + queryString);
		records.getResult().forEach(System.out::println);

	}
}
