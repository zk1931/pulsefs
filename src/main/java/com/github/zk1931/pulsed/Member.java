package com.github.zk1931.pulsed;

import com.google.gson.annotations.Expose;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a member in pulsed.
 */
public final class Member implements Delayed, Comparable<Delayed> {
  private static final Logger LOG = LoggerFactory.getLogger(Member.class);
  @Expose(serialize = false) final String name;
  @Expose final String owner;
  @Expose final boolean active;
  @Expose(serialize = false) final long delayNs;

  public Member(String name, String owner, boolean active, long delaySec) {
    this.name = name;
    this.owner = owner;
    this.active = active;
    this.delayNs = System.nanoTime() + delaySec * 1000 * 1000 * 1000;
  }

  public Member(String name, long delaySec) {
    this.name = name;
    this.owner = "";
    this.active = false;
    this.delayNs = System.nanoTime() + delaySec * 1000 * 1000 * 1000;
  }

  @Override
  public int compareTo(Delayed that) {
    long diff = this.getDelay(TimeUnit.NANOSECONDS) -
                that.getDelay(TimeUnit.NANOSECONDS);
    return (diff == 0) ? 0 : (diff < 0) ? -1 : 1;
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(delayNs - System.nanoTime(), TimeUnit.NANOSECONDS);
  }

  @Override
  public boolean equals(Object obj){
    if(obj instanceof Member) {
      Member that = (Member)obj;
      return this.name.equals(that.name);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  public String toString() {
    return String.format("Member:owner=%s,active=%b", owner, active);
  }
}
