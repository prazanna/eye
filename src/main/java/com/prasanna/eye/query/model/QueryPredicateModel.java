package com.prasanna.eye.query.model;

public class QueryPredicateModel<P,V> {
	private final PredicateFunction function;
	private final PredicateProperty<P> predicateProperty;
	private final PredicateValue<V> predicateValue;

	public QueryPredicateModel(final String function, final PredicateProperty<P> predicateProperty, final PredicateValue<V> predicateValue) {
		this.function = PredicateFunction.valueOf(function);
    this.predicateProperty = predicateProperty;
		this.predicateValue = predicateValue;
	}

  public PredicateFunction getFunction() {
    return function;
  }

  public PredicateProperty<P> getPredicateProperty() {
    return predicateProperty;
  }

  public PredicateValue<V> getPredicateValue() {
    return predicateValue;
  }

  @Override
  public String toString() {
    return "QueryPredicateModel{" +
        "function='" + function + '\'' +
        ", predicateProperty=" + predicateProperty +
        ", predicateValue=" + predicateValue +
        '}';
  }
}
