package com.db.influxdb;

import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

public class ResultSet {

	private List<Result> results;

	private String error;

	public class Result {

		private List<Series> series;

		private String error;

		public class Series {

			private String name;

			private List<String> columns;
			
//			private Map<String, String> tags;

//			private List<List<Object>> values;
			private List<List<Object>> values;
			
			public String getName() {
				return name;
			}

			public void setName(String name) {
				this.name = name;
			}

			public List<String> getColumns() {
				return columns;
			}

//			public Map<String, String> getTags() {
//				return tags;
//			}
//
//			public void setTags(Map<String, String> tags) {
//				this.tags = tags;
//			}

			public List<List<Object>> getValues() {
				return values;
			}
			
			@Override
			public String toString() {
//				return "Series [name=" + name + ", columns=" + columns + ", tags=" + tags + ", values=" + values + "]";
//				return "name=" + name + ", columns=" + columns + ", values=" + values;
//				System.out.println(values.size());				
				
				return values.toString();
			}

		}

		public List<Series> getSeries() {
			return series;
		}

		public String getError() {
			return error;
		}

		@Override
		public String toString() {
//			return "Result [series=" + series + ", error=" + error + "]";	
//			return series.toString();	
			return "" + series ;
		}

	}

	public List<Result> getResults() {
		return results;
	}

	public String getError() {
		return error;
	}

	@Override
	public String toString() {
//		return "ResultSet [results=" + results + ", error=" + error + "]";
		return results.toString();
	}

}
