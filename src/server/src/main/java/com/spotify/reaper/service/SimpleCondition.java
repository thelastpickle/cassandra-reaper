/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.reaper.service;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

// fulfils the Condition interface without spurious wakeup problems
// (or lost notify problems either: that is, even if you call await()
// _after_ signal(), it will work as desired.)
final class SimpleCondition implements Condition {

  private boolean set;

  @Override
  public synchronized void await() throws InterruptedException {
    while (!set) {
      wait();
    }
  }

  @Override
  public synchronized boolean await(long time, TimeUnit unit) throws InterruptedException {
    long start = System.nanoTime();
    long timeout = unit.toNanos(time);
    long elapsed;
    while (!set && (elapsed = System.nanoTime() - start) < timeout) {
      TimeUnit.NANOSECONDS.timedWait(this, timeout - elapsed);
    }
    return set;
  }

  public synchronized void reset() {
    set = false;
  }

  @Override
  public void signal() {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void signalAll() {
    set = true;
    notifyAll();
  }

  public synchronized boolean isSignaled() {
    return set;
  }

  @Override
  public void awaitUninterruptibly() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long awaitNanos(long nanosTimeout) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean awaitUntil(Date deadline) throws InterruptedException {
    throw new UnsupportedOperationException();
  }
}
