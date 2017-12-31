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


import java.io.*;
import java.util.zip.GZIPOutputStream;

/**
 * Handles serializable object, optionally gzipped.
 *
 * @see java.io.Serializable
 * @see org.atri.platodb.entity.serialization.SerializableUnmarshaller
 * @author atri
 * @since 2017-mar-17 06:34:08
 */
public class SerializableMarshaller extends Marshaller  {

  /** if true, output will be gzipped */
  private boolean usingCompression = false;

  public SerializableMarshaller() {
  }

  public SerializableMarshaller(boolean usingCompression) {
    this.usingCompression = usingCompression;
  }

  public boolean isUsingCompression() {
    return usingCompression;
  }

  public void setUsingCompression(boolean usingCompression) {
    this.usingCompression = usingCompression;
  }

  public byte[] marshall(Object object) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(2048);

    OutputStream out;
    if (usingCompression) {
      out = new GZIPOutputStream(baos);
    } else {
      out = baos;
    }

    ObjectOutputStream oos = new ObjectOutputStream(out);
    oos.writeObject(object);
    oos.close();
    return baos.toByteArray();
  }

}