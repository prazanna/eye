package com.prasanna.eye.query;

public class IllegalQueryException extends RuntimeException {
  public IllegalQueryException(final String query, final Throwable e) {
    super("Query " + query + " could not be parsed. ", e);
  }

  public IllegalQueryException(final String message) {
    super(message);
  }
}
