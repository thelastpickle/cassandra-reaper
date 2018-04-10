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

package io.cassandrareaper.jmx;

import java.util.concurrent.Future;
import javax.management.openmbean.CompositeData;

public interface StreamStatusHandler {

  /**
   * Handle the notification about an event related to Cassandra streaming.
   *  @param clusterName name of the cluster where the streaming event occurred
   * @param host name of the node originating the notification
   * @param payload payload attached to the notification by Cassandra
   * @param timeStamp of the JMX notification
   */
  Future<?> handleNotification(String clusterName, String host, CompositeData payload, long timeStamp);

}
