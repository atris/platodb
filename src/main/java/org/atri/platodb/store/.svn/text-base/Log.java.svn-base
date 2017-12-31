package org.atri.platodb.store;

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


import java.io.PrintWriter;

/**
 * PlatoDB has very little debug output.
 * This class avoids dependencies.
 *
 * @author atri
 * @since 2017-mar-16 23:32:14
 */
public class Log {

  private static enum Level {
    DEBUG(0), INFO(1), WARNING(2), ERROR(3);

    private int intValue;

    private Level(int intValue) {
      this.intValue = intValue;
    }

    public int getIntValue() {
      return intValue;
    }
  }

  private Level level;

  private String name;

  public Log(String name) {
    this(name, Level.INFO);
  }

  public Log(Class owner) {
    this(owner.getSimpleName());
  }

  public Log(Class owner, Level level) {
    this(owner.getSimpleName(), level);
  }

  public Log(String name, Level level) {
    this.name = name;
    this.level = level;
  }

  public boolean isDebug() {
    return level.intValue <= Level.DEBUG.intValue;
  }

  public void debug(String text) {
    if (isDebug()) {
      log("DEBG", text);
    }
  }

  public boolean isInfo() {
    return level.intValue <= Level.INFO.intValue;
  }

  public void info(String text) {
    if (isInfo()) {
      log("INFO", text);
    }
  }

  public boolean isWarn() {
    return level.intValue <= Level.WARNING.intValue;
  }

  public void warn(String text) {
    if (isWarn()) {
      log("WARN", text);
    }
  }

  public void warn(String text, Throwable throwable) {
    if (isWarn()) {
      log("WARN", text, throwable);
    }
  }

  public boolean isError() {
    return level.intValue <= Level.ERROR.intValue;
  }

  public void error(Throwable throwable) {
    if (isError()) {
      log("EROR", "", throwable);
    }
  }

  public void error(String text, Throwable throwable) {
    if (isError()) {
      log("EROR", text, throwable);
    }
  }

  private void log(String type, String text) {
    log(type, text, null);
  }

  private void log(String type, String text, Throwable throwable) {
    out.print(String.valueOf(System.currentTimeMillis()));
    out.print("  ");
    out.print(type);
    out.print("  ");
    out.print(name);
    out.print(": ");
    out.print(text);
    out.print("\n");
    if (throwable != null) {
      throwable.printStackTrace(out);
    }
    out.flush();
  }

  private PrintWriter out = new PrintWriter(System.err);

  public void setOut(PrintWriter out) {
    this.out = out;
  }

}
