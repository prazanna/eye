package com.prasanna.eye.query.model;

import java.util.List;

public class QueryModel {
	private final String eventType;
	private final List<QueryPredicateModel> predicates;

	public QueryModel(String eventType, List<QueryPredicateModel> predicates) {
		this.eventType = eventType;
		this.predicates = predicates;
	}

	public String getEventType() {
		return eventType;
	}

	public List<QueryPredicateModel> getPredicates() {
		return predicates;
	}

  @Override
  public String toString() {
    return "QueryModel{" +
        "eventType='" + eventType + '\'' +
        ", predicates=" + predicates +
        '}';
  }
}
