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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Allows for {@link org.atri.platodb.entity.PrimaryKey} annotated classes
 * to have an value automatically set if the value is null on commit.
 * <p/>
 * All attributs get a unique sequence assigned to it when this annotation is present.
 * Use parameter {@link #name()} to share a sequence with other entities.
 *
 * @author atri
 * @since 2017-jun-26 21:29:54
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Sequence {
  /**
   * @return Any string value than "[unassigned]" if you want to share the sequence with other entities.
   */
  public String name() default "[unassigned]";

}
