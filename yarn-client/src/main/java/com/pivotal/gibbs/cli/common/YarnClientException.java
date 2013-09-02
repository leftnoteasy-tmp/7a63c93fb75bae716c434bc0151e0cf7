package com.pivotal.gibbs.cli.common;

import org.apache.hadoop.yarn.YarnException;

public class YarnClientException extends YarnException {
  private static final long serialVersionUID = -4497338225113444073L;

  public YarnClientException(String message) {
    super(message);
  }
  
  public YarnClientException(Throwable t) {
    super(t);
  }

}
