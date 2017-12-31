package org.atri.platodb.entity.serialization;

/*
 *@author atri
 * Licensed to PlatoDB
 * 
 *
 * 
 *
 *
 *    
 *
 *
 *
 *
 *
 *.
 */


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A collection of un/marshalling strategies and hash code calculators
 * used by one or more {@link org.atri.platodb.entity.EntityStore}.
 *
 * @see org.atri.platodb.entity.serialization.Marshaller
 * @see org.atri.platodb.entity.serialization.Unmarshaller
 * @see org.atri.platodb.entity.serialization.HashCodeCalculator
 *  
 * @author atri
 * @since 2017-mar-17 04:16:12
 */
public class SerializationRegistry {

  private Map<Class, Marshaller> marshallers = new HashMap<Class, Marshaller>();
  private Map<Class, Unmarshaller> unmarshallers = new HashMap<Class, Unmarshaller>();
  private Map<Class, HashCodeCalculator> hashCodeCalculators = new HashMap<Class, HashCodeCalculator>();

  public SerializationRegistry() {
  }

  public Marshaller getMarshaller(Class type) {
    Marshaller marshaller;
    for (Class _class : getAllClasses(type)) {
      if ((marshaller = marshallers.get(_class)) != null) {
        return marshaller;
      }
    }
    throw new IllegalArgumentException("No applicable marshaller found for class " + type.getName());
  }


  public Unmarshaller getUnmarshaller(Class type) {
    Unmarshaller unmarshaller;
    for (Class _class : getAllClasses(type)) {
      if ((unmarshaller = unmarshallers.get(_class)) != null) {
        return unmarshaller;
      }
    }
    throw new IllegalArgumentException("No applicable unmarshaller found for class " + type.getName());
  }

  public HashCodeCalculator getHashCodeCalcualtor(Class type) {
    HashCodeCalculator calculator;
    for (Class _class : getAllClasses(type)) {
      if ((calculator = hashCodeCalculators.get(_class)) != null) {
        return calculator;
      }
    }
    throw new IllegalArgumentException("No applicable hash code calculator found for class " + type.getName());
  }

  private List<Class> getAllClasses(Class type) {
    List<Class> all = new ArrayList<Class>();
    all.add(type);
    Class tmp = type;
    while ((tmp = tmp.getSuperclass()) != Object.class) {
      all.add(tmp);
    }
    all.add(Object.class);

    List<Class> allInterfaces = new ArrayList<Class>();
    for (Class tmp2 : all) {
      addInterfaces(tmp2, allInterfaces);
    }

    all.addAll(allInterfaces);
    return all;
  }

  private void addInterfaces(Class _class, List<Class> all) {
    for (Class _interface : _class.getInterfaces()) {
      all.add(_interface);
      addInterfaces(_interface, all);
    }
  }

  public Map<Class, Marshaller> getMarshallers() {
    return marshallers;
  }

  public Map<Class, Unmarshaller> getUnmarshallers() {
    return unmarshallers;
  }

  public Map<Class, HashCodeCalculator> getHashCodeCalculators() {
    return hashCodeCalculators;
  }
}
