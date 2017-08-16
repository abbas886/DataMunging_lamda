package com.stackroute.datamunging;

import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;

import com.stackroute.query.parser.AggregateFunction;

public class ResultSet {

	private List<List<String>> result;

	private List<AggregateFunction> aggregateFunctions;

	private Map<String, List<List<String>>> groupByResult;
	
	private Map<String, DoubleSummaryStatistics> groupByAggregateResult;
	
	

	public Map<String, DoubleSummaryStatistics> getGroupByAggregateResult() {
		return groupByAggregateResult;
	}

	public void setGroupByAggregateResult(Map<String, DoubleSummaryStatistics> groupByAggregateResult) {
		this.groupByAggregateResult = groupByAggregateResult;
	}

	public Map<String, List<List<String>>> getGroupByResult() {
		return groupByResult;
	}

	public void setGroupByResult(Map<String, List<List<String>>> groupByResult) {
		this.groupByResult = groupByResult;
	}

	public List<List<String>> getResult() {
		return result;
	}

	public void setResult(List<List<String>> result) {
		this.result = result;
	}

	public List<AggregateFunction> getAggregateFunctions() {
		return aggregateFunctions;
	}

	public void setAggregateFunctions(List<AggregateFunction> aggregateFunctions) {
		this.aggregateFunctions = aggregateFunctions;
	}

}
