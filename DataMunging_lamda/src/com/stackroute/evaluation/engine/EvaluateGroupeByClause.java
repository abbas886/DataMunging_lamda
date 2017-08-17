package com.stackroute.evaluation.engine;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.DoubleSummaryStatistics;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.stackroute.datamunging.ResultSet;
import com.stackroute.datamunging.util.FilterHandler;
import com.stackroute.query.parser.AggregateFunction;
import com.stackroute.query.parser.QueryParameter;

public class EvaluateGroupeByClause implements EvaluateEngine {
	private ResultSet resultSet;
	private List<String> record;
	private Map<String, Integer> header;
	private FilterHandler filterHandler;

	@Override
	public ResultSet evaluate(QueryParameter queryParameter) {
		resultSet = new ResultSet();
		header = queryParameter.getHeader();
		filterHandler = new FilterHandler();
		List<List<String>> result = new ArrayList<List<String>>();
		List<String> selectedFields = queryParameter.getFields();
		try (BufferedReader reader = new BufferedReader(new FileReader(queryParameter.getFile()))) {
			// read header
			reader.readLine().split(",");
			String line;
			// read the remaining records
			while ((line = reader.readLine()) != null) {
				record = Arrays.asList(line.split(","));

				if (filterHandler.isRequiredRecord(queryParameter, record)) {
					if (!selectedFields.get(0).equals("*")) {
						record = filterHandler.filterFields(queryParameter, record);
					}
					result.add(record);
				}
				

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		if (queryParameter.getFields() == null || queryParameter.getAggregateFunctions() == null) {
			resultSet.setGroupByResult(calclulateGroupeByWithFields(result, queryParameter));

		} else {
			resultSet.setGroupByAggregateResult(calclulateGroupeByWithAggregates(result, queryParameter));

		}

		return resultSet;
	}

	private Map<String, List<List<String>>> calclulateGroupeByWithFields(List<List<String>> result,
			QueryParameter queryParameter) {
	//	Map<String, Integer> header = queryParameter.getHeader();
		String groupByField = queryParameter.getGroupByFields().get(0);
		List<String> selectedFields = queryParameter.getFields();
		int groupByFieldIndex = getGroupByFieldIndex(selectedFields, groupByField);
		Map<String, List<List<String>>> groupBy = result.stream()
				.collect(Collectors.groupingBy((record) -> record.get(groupByFieldIndex)));

		return groupBy;
	}

	private int getGroupByFieldIndex(List<String> selectedFields, String groupByField) {
		int noOfFields = selectedFields.size();
		for(int index = 0; index<noOfFields; index ++)
		{
			if(selectedFields.get(index).equals(groupByField))
			{
				return index;
			}
		}
		// TODO Auto-generated method stub
		return 0;
	}

	private Map<String, DoubleSummaryStatistics> calclulateGroupeByWithAggregates(List<List<String>> result,
			QueryParameter queryParameter) {

		Map<String, Integer> header = queryParameter.getHeader();
		int groupByFieldIndex = header.get(queryParameter.getGroupByFields().get(0));
		List<AggregateFunction> aggregates = queryParameter.getAggregateFunctions();
		AggregateFunction aggregate = aggregates.get(0); // only one aggregate
		int aggregateFieldIndex;
		if (aggregate.getField().equals("*")) {
			aggregateFieldIndex = 0;
		} else {
			aggregateFieldIndex = header.get(aggregate.getField());

		}

		Map<String, DoubleSummaryStatistics> groupByStatistics = null;
		try {
			groupByStatistics = result.stream().collect(Collectors.groupingBy((record) -> record.get(groupByFieldIndex),
					Collectors.summarizingDouble(record -> Integer.parseInt(record.get(aggregateFieldIndex)))));
		} catch (NumberFormatException e) {

			e.printStackTrace();
		}

		return groupByStatistics;
	}

}
