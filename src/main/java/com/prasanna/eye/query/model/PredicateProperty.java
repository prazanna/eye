package com.prasanna.eye.query.model;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import com.prasanna.eye.query.IllegalQueryException;

public class PredicateProperty<T> {
  public enum Type {
    PATH(String.class), TEXT(String.class);

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

  private final T property;
  private final Type type;

  public PredicateProperty(final T property, final Type type) {
    if (!type.isClassSupported(property.getClass())) {
      throw new IllegalQueryException("Property of type " + type.name() + " should not be an instance of " + property
          .getClass() + ". Supported types are " + type.getSupportedClasses());
    }
    this.property = property;
    this.type = type;
  }

  public T getProperty() {
    return property;
  }

  public Type getType() {
    return type;
  }

  @Override
  public String toString() {
    return "PredicateProperty{" +
        "property=" + property +
        ", type=" + type +
        '}';
  }


}
