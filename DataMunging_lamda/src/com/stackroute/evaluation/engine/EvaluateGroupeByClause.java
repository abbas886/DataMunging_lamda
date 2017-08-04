package com.stackroute.evaluation.engine;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.stackroute.datamunging.ResultSet;
import com.stackroute.query.parser.AggregateFunction;
import com.stackroute.query.parser.QueryParameter;

public class EvaluateGroupeByClause implements EvaluateEngine{
	private ResultSet resultSet;

	@Override
	public ResultSet evaluate(QueryParameter queryParameter) {
		Map<String, List<AggregateFunction>> groupByResult;

		resultSet = new ResultSet();
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
			//groupByResult =calclulateAggregates(result,queryParameter );

			resultSet.setResult(result);
		return resultSet;
	}

	/*private Map<String, List<AggregateFunction>> calclulateAggregates(List<List<String>> result,
			QueryParameter queryParameter) {
		Map<String, Integer> header = queryParameter.getHeader();
		String groupByField = queryParameter.getGroupByFields().get(0);
		Map<String, Long> counting = result.stream().collect(
	                Collectors.groupingBy(result.get(0).get(header.get(groupByField)), Collectors.counting()));

	
		return null;
	}*/
	
	
	
	
}
