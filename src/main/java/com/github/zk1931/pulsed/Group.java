package com.github.zk1931.pulsed;

import com.google.gson.annotations.Expose;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a group in pulsed.
 */
public final class Group {
  private static final Logger LOG = LoggerFactory.getLogger(Group.class);
  @Expose
  final ConcurrentMap<String, Member> members = new ConcurrentHashMap<>();
}
