package com.prasanna.eye.storage.db.hbase.filters;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import com.jayway.jsonpath.JsonPath;
import com.prasanna.eye.http.model.TimedEvent;
import com.prasanna.eye.query.IllegalQueryException;
import com.prasanna.eye.query.model.PredicateProperty;
import com.prasanna.eye.query.model.PredicateValue;
import com.prasanna.eye.query.model.QueryPredicateModel;
import com.prasanna.eye.storage.EventFilter;

public class HBaseEventDefaultFilter implements EventFilter<Scan, TimedEvent> {
  private static ObjectMapper objectMapper = new ObjectMapper();
  private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss z");

  public void applyRowFilters(final Scan scan, final List<QueryPredicateModel> predicateModels) {
    if (predicateModels.isEmpty()) {
      return;
    }

    Iterator<QueryPredicateModel> predicateIterator = predicateModels.iterator();
    while (predicateIterator.hasNext()) {
      QueryPredicateModel predicate = predicateIterator.next();
      switch (predicate.getFunction()) {
        case within:
          WithinFunctionFilter.applyFilter(predicate, scan);
          predicateIterator.remove();
          break;
        case last:
          LastFunctionFilter.applyFilter(predicate, scan);
          predicateIterator.remove();
      }
    }
  }

  public boolean applyDataFilters(final TimedEvent timedEvent,
                                  final List<QueryPredicateModel> predicateModels) {
    if(predicateModels.isEmpty()) {
      return true;
    }

    JsonNode dataJson;
    try {
      dataJson = objectMapper.readTree(timedEvent.getData());
    } catch (IOException e) {
      throw new InternalError("Error while trying to convert timed event data to Json " + e.toString());
    }

    Iterator<QueryPredicateModel> predicateIterator = predicateModels.iterator();
    while (predicateIterator.hasNext()) {
      QueryPredicateModel predicate = predicateIterator.next();
      boolean filterResult;
      switch (predicate.getFunction()) {
        case eq:
          filterResult = EqualFunctionFilter.applyFilter(dataJson, predicate);
          predicateIterator.remove();
          break;
        case re:
          filterResult = REFunctionFilter.applyFilter(dataJson, predicate);
          predicateIterator.remove();
          break;
        default:
          throw new NotImplementedException("Predicate model " + predicate.getFunction() + " is not yet implemented " +
              "for HBase storage");

      }
      if (filterResult) {
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
        throw new IllegalQueryException("Could not parse within property to a date " + property.getProperty() + ", " +
            "format expected is " + dateFormat.toPattern(), e);
      }

      PredicateValue predicateValue = predicateModel.getPredicateValue();
      checkValueType(predicateValue.getType(), PredicateValue.Type.TEXT,
          "Within (within)");
      Long to;
      try {
        to = dateFormat.parse((String) predicateValue.getValue()).getTime();
      } catch (ParseException e) {
        throw new IllegalQueryException("Could not parse within value to a date " + property.getProperty() + ", " +
            "format expected is " + dateFormat.toPattern(), e);
      }

      long alreadySetStart = Bytes.toLong(scan.getStartRow());
      long revFrom = Long.MAX_VALUE - from;
      long revTo = Long.MAX_VALUE - to;

      if (revTo > alreadySetStart) {
        scan.setStartRow(Bytes.toBytes(revTo));
      }
      long alreadySetEnd = Bytes.toLong(scan.getStopRow());
      if (revFrom < alreadySetEnd) {
        scan.setStopRow(Bytes.toBytes(revFrom));
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
