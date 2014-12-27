/**
 * Licensed to the zk9131 under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.zk1931.pulsefs;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Session object.
 */
public class Session implements Delayed {

  final long delayNs;
  final long sessionID;

  public Session(long sessionID, long delaySec) {
    this.sessionID = sessionID;
    this.delayNs = System.nanoTime() + delaySec * 1000 * 1000 * 1000;
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(delayNs - System.nanoTime(), TimeUnit.NANOSECONDS);
  }

  @Override
  public int compareTo(Delayed that) {
    long diff = this.getDelay(TimeUnit.NANOSECONDS) -
                that.getDelay(TimeUnit.NANOSECONDS);
    return (diff == 0) ? 0 : (diff < 0) ? -1 : 1;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Session) {
      Session that = (Session)obj;
      return this.sessionID == that.sessionID;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Long.valueOf(sessionID).hashCode();
  }

  @Override
  public String toString() {
    return String.format("Session: ID=%s", this.sessionID);
  }
}
