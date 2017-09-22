/*
 * Copyright (c) 2017-2017 Globo.com
 * All rights reserved.
 *
 * This source is subject to the Apache License, Version 2.0.
 * Please see the LICENSE file for more information.
 *
 * Authors: See AUTHORS file
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.galeb.statsd;

@SuppressWarnings("unused")
public interface StatsDClient {

    void stop();

    void count(String aspect, int delta, String... tags);

    void incrementCounter(String aspect, String... tags);

    void increment(String aspect, String... tags);

    void decrementCounter(String aspect, String... tags);

    void decrement(String aspect, String... tags);

    void recordGaugeValue(String aspect, double value, String... tags);

    void gauge(String aspect, double value, String... tags);

    void recordGaugeValue(String aspect, int value, String... tags);

    void gauge(String aspect, int value, String... tags);

    void recordExecutionTime(String aspect, long timeInMs, String... tags);

    void time(String aspect, long value, String... tags);

    void recordHistogramValue(String aspect, double value, String... tags);

    void histogram(String aspect, double value, String... tags);

    void recordHistogramValue(String aspect, int value, String... tags);

    void histogram(String aspect, int value, String... tags);

}
