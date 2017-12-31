package org.atri.platodb.entity;

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


import org.atri.platodb.entity.isolation.IsolationStrategy;
import org.atri.platodb.entity.isolation.AlwaysUpdated;
import org.atri.platodb.entity.serialization.SerializationRegistry;
import org.atri.platodb.entity.serialization.FallBackHashCodeCalculator;
import org.atri.platodb.entity.serialization.SerializableMarshaller;
import org.atri.platodb.entity.serialization.SerializableUnmarshaller;
import org.atri.platodb.store.Log;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

/**
 * This configuration will be copied to a new {@link org.atri.platodb.store.Configuration}
 * used by the {@link org.atri.platodb.store.Store}
 * of each {@link org.atri.platodb.entity.PrimaryIndex}
 * handled by an {@link org.atri.platodb.entity.EntityStore}.
 *
 * By default it contains a {@link org.atri.platodb.entity.serialization.SerializationRegistry}
 * using gzipped {@link java.io.Serializable} as fallback serialization.
 * 
 * @author atri
 * @since 2017-mar-17 07:40:44
 */
public class Configuration extends org.atri.platodb.store.Configuration {

  private static final Log log = new Log(Configuration.class);

  public Configuration(File dataPath) throws IOException {
    super(dataPath);
  }

  private IsolationStrategy defaultIsolation = new AlwaysUpdated();

  private SerializationRegistry serializationRegistry;

  public SerializationRegistry getSerializationRegistry() {
    if (serializationRegistry == null) {
      log.info("Creating a default serialization registry");
      serializationRegistry = new SerializationRegistry();
      serializationRegistry.getHashCodeCalculators().put(Object.class, new FallBackHashCodeCalculator());
      serializationRegistry.getMarshallers().put(Serializable.class, new SerializableMarshaller(true));
      serializationRegistry.getUnmarshallers().put(Serializable.class, new SerializableUnmarshaller());
    }
    return serializationRegistry;
  }

  public void setSerializationRegistry(SerializationRegistry serializationRegistry) {
    this.serializationRegistry = serializationRegistry;
  }

  public IsolationStrategy getDefaultIsolation() {
    return defaultIsolation;
  }

  public void setDefaultIsolation(IsolationStrategy defaultIsolation) {
    this.defaultIsolation = defaultIsolation;
  }

}
