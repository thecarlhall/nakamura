package org.sakaiproject.nakamura.media;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Slowdown {

  private long delay;
  private long lastTime = -1;

  private static final Logger LOGGER = LoggerFactory
    .getLogger(Slowdown.class);


  public Slowdown(long delay) {
    this.delay = delay;
  }


  public void sleep() {
    long now = System.currentTimeMillis();

    if (lastTime > 0) {
      long elapsed = (now - lastTime);
      if (elapsed < delay) {
        try {
          LOGGER.info("Waiting...");
          Thread.sleep(delay - elapsed);
        } catch (InterruptedException e) {}
      }
    } else {
      lastTime = now;
    }
  }
}
