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
 * PlatoDB use both a long hash code in the persistent store
 * and the plain old integer hash code during transactions.
 *
 * This class is used to calculate these values.
 *
 * @author atri
 * @since 2017-mar-17 03:59:08
 */
public abstract class HashCodeCalculator {

  public abstract long calcualteLongHashCode(Object object);
  public abstract int calcualteIntegerHashCode(Object object);

}
