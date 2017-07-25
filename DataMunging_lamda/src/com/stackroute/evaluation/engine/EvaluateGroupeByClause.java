package com.stackroute.evaluation.engine;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.stackroute.datamunging.ResultSet;
import com.stackroute.query.parser.QueryParameter;

public class EvaluateGroupeByClause implements EvaluateEngine{
	private ResultSet resultSet;

	@Override
	public ResultSet evaluate(QueryParameter queryParameter) {
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
			resultSet.setResult(result);
		return resultSet;
	}
	
	
	
	
}
