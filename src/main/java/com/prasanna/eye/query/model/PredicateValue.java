package com.prasanna.eye.query.model;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import com.prasanna.eye.query.IllegalQueryException;

public class PredicateValue<T> {
  public enum Type {
    TEXT(String.class), REGEX(Pattern.class), TEXT_ARRAY(Collection.class);
    private Set<Class> supportedClasses;

    Type(Class... supportedClass) {
      this.supportedClasses = new HashSet<Class>();
      Collections.addAll(this.supportedClasses, supportedClass);
    }

    public boolean isClassSupported(Class clazz) {
      return this.supportedClasses.contains(clazz);
    }

    public Set<Class> getSupportedClasses() {
      return supportedClasses;
    }
  }

  private final T value;
  private final Type type;

  public PredicateValue(final T value, final Type type) {
    if (!type.isClassSupported(value.getClass())) {
      throw new IllegalQueryException("Value of type " + type.name() + " should not be an instance of " + value
          .getClass() + ". Supported types are " + type.getSupportedClasses());
    }
    this.value = value;
    this.type = type;
  }

  public T getValue() {
    return value;
  }

  public Type getType() {
    return type;
  }

  @Override
  public String toString() {
    return "PredicateValue{" +
        "value=" + value +
        ", type=" + type +
        '}';
  }
}
