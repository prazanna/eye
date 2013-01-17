package com.prasanna.eye.storage.db.hbase.filters;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import com.jayway.jsonpath.JsonPath;
import com.prasanna.eye.http.model.TimedEvent;
import com.prasanna.eye.query.IllegalQueryException;
import com.prasanna.eye.query.model.PredicateProperty;
import com.prasanna.eye.query.model.PredicateValue;
import com.prasanna.eye.query.model.QueryModel;
import com.prasanna.eye.query.model.QueryPredicateModel;
import com.prasanna.eye.storage.EventFilter;

public class HBaseEventDefaultFilter implements EventFilter<Scan, TimedEvent> {
  private static ObjectMapper objectMapper = new ObjectMapper();
  private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss z");

  public void applyRowFilters(final Scan scan, final QueryModel eventQueryModel) {
    for (QueryPredicateModel predicate : eventQueryModel.getPredicates()) {
      switch (predicate.getFunction()) {
        case within:
          WithinFunctionFilter.applyFilter(predicate, scan);
          break;
        case last:
          LastFunctionFilter.applyFilter(predicate, scan);
      }
    }
  }

  public boolean applyDataFilters(final TimedEvent timedEvent,
                                  final QueryModel eventQueryModel) {
    JsonNode dataJson;
    try {
      dataJson = objectMapper.readTree(timedEvent.getData());
    } catch (IOException e) {
      throw new InternalError("Error while trying to convert timed event data to Json " + e.toString());
    }


    for (QueryPredicateModel predicateModel : eventQueryModel.getPredicates()) {
      boolean filterResult;
      switch (predicateModel.getFunction()) {
        case eq:
          filterResult = EqualFunctionFilter.applyFilter(dataJson,
              predicateModel);
          break;
        case re:
          filterResult = REFunctionFilter.applyFilter(dataJson,
              predicateModel);
          break;
        default:
          throw new NotImplementedException("Predicate model " + predicateModel.getFunction() + " is not yet implemented for HBase storage");

      }
      if(filterResult) {
        return true;
      }
    }
    return false;
  }

  static class WithinFunctionFilter {
    static void applyFilter(final QueryPredicateModel predicateModel,
                            final Scan scan) {
      PredicateProperty property = predicateModel.getPredicateProperty();
      checkPropertyType(property.getType(), PredicateProperty.Type.TEXT, "Within (within)");
      Long from;
      try {
        from = dateFormat.parse((String) property.getProperty()).getTime();
      } catch (ParseException e) {
        throw new IllegalQueryException("Could not parse within property to a date " + property.getProperty()+", format expected is " + dateFormat.toPattern(), e);
      }

      PredicateValue predicateValue = predicateModel.getPredicateValue();
      checkValueType(predicateValue.getType(), PredicateValue.Type.TEXT,
          "Within (within)");
      Long to;
      try {
        to = dateFormat.parse((String) predicateValue.getValue()).getTime();
      } catch (ParseException e) {
        throw new IllegalQueryException("Could not parse within value to a date " + property.getProperty()+", format expected is " + dateFormat.toPattern(), e);
      }

      long alreadySetStart = Bytes.toLong(scan.getStartRow());
      long revFrom = Long.MAX_VALUE - from;
      long revTo = Long.MAX_VALUE - to;

      if(revFrom > alreadySetStart) {
        scan.setStartRow(Bytes.toBytes(revFrom));
      }
      long alreadySetEnd = Bytes.toLong(scan.getStopRow());
      if(revTo < alreadySetEnd) {
        scan.setStopRow(Bytes.toBytes(revTo));
      }
    }
  }

  static class LastFunctionFilter {
    static void applyFilter(final QueryPredicateModel predicateModel,
                            final Scan scan) {

    }
  }

  static class EqualFunctionFilter {
    static boolean applyFilter(final JsonNode dataNode,
                               final QueryPredicateModel predicateModel) {
      PredicateProperty property = predicateModel.getPredicateProperty();
      checkPropertyType(property.getType(), PredicateProperty.Type.PATH, "Equals (eq)");
      String path = (String) property.getProperty();

      PredicateValue predicateValue = predicateModel.getPredicateValue();
      checkValueType(predicateValue.getType(), PredicateValue.Type.TEXT,
          "Equals (eq)");
      String value = (String) predicateValue.getValue();

      String readValue = JsonPath.read(dataNode, path);
      return readValue.equalsIgnoreCase(value);
    }
  }

  static class REFunctionFilter {
    static boolean applyFilter(final JsonNode dataNode,
                               final QueryPredicateModel predicateModel) {
      PredicateProperty property = predicateModel.getPredicateProperty();
      checkPropertyType(property.getType(), PredicateProperty.Type.PATH, "Regular Expression (re)");
      String path = (String) property.getProperty();

      PredicateValue predicateValue = predicateModel.getPredicateValue();
      checkValueType(predicateValue.getType(), PredicateValue.Type.TEXT,
          "Equals (eq)");
      String value = (String) predicateValue.getValue();
      String readValue = JsonPath.read(dataNode, path);
      return readValue.matches(value);
    }
  }

  private static void checkPropertyType(final PredicateProperty.Type actual,
                                        final PredicateProperty.Type expected, final String filter) {
    if (actual != expected) {
      throw new IllegalQueryException(filter + " filter should have a "
          + expected + " as its property. Illegal property " + actual);
    }
  }

  private static void checkValueType(final PredicateValue.Type actual,
                                     final PredicateValue.Type expected, final String filter) {
    if (actual != expected) {
      throw new IllegalQueryException(filter + " filter should have a "
          + expected + " as its value. Illegal value " + actual);
    }
  }
}
