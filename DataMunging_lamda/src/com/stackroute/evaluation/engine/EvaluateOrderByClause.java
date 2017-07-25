package com.stackroute.evaluation.engine;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.stackroute.datamunging.Query;
import com.stackroute.datamunging.ResultSet;
import com.stackroute.datamunging.util.FilterHandler;
import com.stackroute.query.parser.QueryParameter;

public class EvaluateOrderByClause implements EvaluateEngine {
	
	@Override
	public ResultSet evaluate(QueryParameter queryParameter) {
		ResultSet resultSet = new ResultSet();
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
			result = sortRecords(queryParameter,result);
			resultSet.setResult(result);
		return resultSet;
	}

	private List<List<String>> sortRecords(QueryParameter queryParameter, List<List<String>> result) {
		int orderByIndex =queryParameter.getHeader().get(queryParameter.getOrderByFields().get(0));
		result.sort((r1, r2) -> r1.get(orderByIndex).compareTo(r2.get(orderByIndex)));
		
		return result;
	}

	
}
