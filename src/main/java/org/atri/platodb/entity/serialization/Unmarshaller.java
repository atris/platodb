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


/**
 * Translate byte arrays to objects.
 *
 * @see org.atri.platodb.entity.serialization.Marshaller
 *
 * @author atri
 * @since 2017-mar-17 03:58:21
 */
public abstract class Unmarshaller {

  public Object unmarshall(byte[] bytes) throws java.io.IOException {
    return unmarshall(bytes, 0, bytes.length);
  }
  
  public abstract Object unmarshall(byte[] bytes, int startOffset, int length) throws java.io.IOException;

}
