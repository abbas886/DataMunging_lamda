package com.stackroute.evaluation.engine;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.stackroute.datamunging.Query;
import com.stackroute.datamunging.ResultSet;
import com.stackroute.datamunging.util.FilterHandler;
import com.stackroute.query.parser.QueryParameter;
import com.stackroute.query.parser.Restriction;

public class EvaluateWhereClauseQuery implements EvaluateEngine {
	private ResultSet resultSet;
	private List<String> record;
	private Map<String, Integer> header;
	private FilterHandler filterHandler;

	@Override
	public ResultSet evaluate(QueryParameter queryParameter) {
		resultSet = new ResultSet();
		header=queryParameter.getHeader();
		filterHandler=new FilterHandler();
		List<List<String>> result = new ArrayList<List<String>>();
		List<String> selectedFields = queryParameter.getFields();
		
		
			try (BufferedReader reader = new BufferedReader(new FileReader(queryParameter.getFile()))) {
			//read header
			reader.readLine().split(",");
			String line;
			// read the remaining records
			while ((line =reader.readLine()) != null) {
				record = Arrays.asList(line.split(","));
				
				if(!selectedFields.get(0).equals("*"))
				{
					record=filterHandler.filterFields(queryParameter,record);
				}
				

				
				if(filterHandler.isRequiredRecord(queryParameter, record))
				{
					result.add(record);
				}

			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			resultSet.setResult(result);
		return resultSet;
	}

	

}
