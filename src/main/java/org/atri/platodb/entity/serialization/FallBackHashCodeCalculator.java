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
 * Uses integer resolution {@link Object#hashCode()}.
 *
 * @author atri
 * @since 2017-mar-17 06:32:30
 */
public class FallBackHashCodeCalculator extends HashCodeCalculator {

  /**
   * @return same as {@link #calcualteIntegerHashCode(Object)}
   */
  public long calcualteLongHashCode(Object object) {
    return calcualteIntegerHashCode(object);
  }

  public int calcualteIntegerHashCode(Object object) {
    return object.hashCode();
  }
}
