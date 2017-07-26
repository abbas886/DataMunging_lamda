package com.stackroute.evaluation.engine;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.stackroute.datamunging.ResultSet;
import com.stackroute.query.parser.AggregateFunction;
import com.stackroute.query.parser.QueryParameter;

public class EvaluateAggregateClause implements EvaluateEngine {
	private ResultSet resultSet;
    private List<AggregateFunction> aggregates;
		@Override
	public ResultSet evaluate(QueryParameter queryParameter) {
			resultSet = new ResultSet();

			aggregates = queryParameter.getAggregateFunctions();
			Map<String, Integer> header = queryParameter.getHeader();
			
			List<List<String>> result = new ArrayList<List<String>>();
				try (BufferedReader reader = new BufferedReader(new FileReader(queryParameter.getFile()))) {
				//read header
				reader.readLine().split(",");
				String line;
				// read the remaining records
				while ((line =reader.readLine()) != null) {
					result.add(Arrays.asList(line.split(",")));
				
				}

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
				// find min, max, sum and count based on given aggregate function
				calclulateAggregates(result,  header,aggregates);

			
				resultSet.setAggregateFunctions(aggregates);
			return resultSet;
		}
		private List<AggregateFunction> calclulateAggregates(List<List<String>> result, Map<String, Integer> header,List<AggregateFunction> aggregates) {
			for (AggregateFunction aggregate : aggregates) {
				
				  IntSummaryStatistics stats = result.stream()
                          .mapToInt((record) -> Integer.parseInt(record.get(header.get(aggregate.getField()))))
                          .summaryStatistics();
				  switch(aggregate.getFunction())
				  {
				  case "sum"  :aggregate.setResult((int)stats.getSum());
				  break;
				  case "max"  :aggregate.setResult((int)stats.getMax());
				  break;
				  case "min"  :aggregate.setResult((int)stats.getMin());
				  break;
				  case "count"  :aggregate.setResult((int)stats.getMax());
				  break;
				  case "avg"  :aggregate.setResult((int)stats.getAverage());
				  }
				  
				 // aggregates.add(aggregate);
				  
			}
				
			 return aggregates;
			
		}

		
		
		
		
		
		
		
		
		
		
		
		
	
	
}
