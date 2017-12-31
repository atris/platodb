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


import java.io.IOException;

/**
 * Translates objects to byte arrays. 
 *
 * @see org.atri.platodb.entity.serialization.Unmarshaller
 *
 * @author atri
 * @since 2017-mar-17 03:44:55
 */
public abstract class Marshaller {
  
  public abstract byte[] marshall(Object object) throws IOException;


}
