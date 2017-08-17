package com.stackroute.datamunging.test;

import static org.junit.Assert.*;

import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;

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
	

	//@Test
	public void getAllRecords() {
		queryString = "select city,winner,team1,team2 from ipl.csv";
		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayRecords(queryString, records);
	}
	
	//@Test
	public void getCountOfCities() {
		queryString = "select count(city) from ipl.csv";
		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayAggregateResult(queryString, records.getAggregateFunctions());
	}
	
	//@Test
	public void getGroupeByAggregateCount() {
		queryString = "select city,count(win_by_wickets) from ipl.csv group by city";
		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayGroupeByAggregateResult(queryString, records.getGroupByAggregateResult());
	}
	
    //@Test
	public void getGroupeByAggregateMax() {
		queryString = "select city,max(win_by_wickets) from ipl.csv group by city";
		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayGroupeByAggregateResult(queryString, records.getGroupByAggregateResult());
	}
	
	@Test
	public void getGroupeByCountStar() {
		queryString = "select city,count(*) from ipl.csv group by city";
		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayGroupeByAggregateResult(queryString, records.getGroupByAggregateResult());
	}
	
	//@Test
	public void getGroupeByAggregateAvg() {
		queryString = "select city,avg(win_by_wickets) from ipl.csv group by city";
		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayGroupeByAggregateResult(queryString, records.getGroupByAggregateResult());
	}
	
	//@Test
	public void getGroupeByAndConditions() {
		queryString = "select city,winner,team1,team2,player_of_match from ipl.csv "
				+ "where season >= 2013 or toss_decision != bat and city = Bangalore group by team1";
		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayGroupeByResult(queryString, records.getGroupByResult());
	}
	
	
	private void displayGroupeByResult(String queryString, Map<String, List<List<String>>> groupByResult) {
		System.out.println("\nGiven Query : " + queryString);
		groupByResult.entrySet().forEach(System.out::println);
				
	}


	//@Test
	public void getGroupeByAggregateSum() {
		queryString = "select city,sum(season) from ipl.csv group by city";
		ResultSet records = query.executeQuery(queryString);
		assertNotNull("filterData", records);
		displayGroupeByAggregateResult(queryString, records.getGroupByAggregateResult());
	}
	
	
	
	
	
	private void displayGroupeByAggregateResult(String queryString,	Map<String, DoubleSummaryStatistics> groupByResult) {
		System.out.println("\nGiven Query : " + queryString);
		
		groupByResult.entrySet().forEach(System.out::println);
		
		// TODO Auto-generated method stub
		
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
