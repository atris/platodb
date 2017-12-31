package org.atri.platodb.entity;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

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
 * Fields annotated with this
 * makes up the unique identification for an {@link org.atri.platodb.entity.Entity}
 * in a {@link org.atri.platodb.entity.PrimaryIndex}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface PrimaryKey {

}
