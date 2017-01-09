/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.breaker;

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;

public class FixedSizeRamAccountingQueueTest extends CrateUnitTest {

    private static class EstimableLong implements Estimable {
        private final long value;

        EstimableLong(long value) {
            this.value = value;
        }

        public long value() {
            return value;
        }

        @Override
        public long estimateSize() {
            return 32; // 24 bytes overhead + 8 bytes long
        }
    }

    @Test
    public void testOffer() throws Exception {
        CircuitBreaker circuitBreaker =  mock(CircuitBreaker.class);
        // mocked CircuitBreaker has unlimited memory (⌐■_■)
        when(circuitBreaker.getLimit()).thenReturn(Long.MAX_VALUE);
        RamAccountingContext context = new RamAccountingContext("testRamAccountingContext", circuitBreaker);
        FixedSizeRamAccountingQueue<EstimableLong> accountingQueue = new FixedSizeRamAccountingQueue<>(context, 15_000);

        int THREADS = 50;
        final CountDownLatch latch = new CountDownLatch(THREADS);
        List<Thread> threads = new ArrayList<>(20);
        for (int i = 0; i < THREADS; i++) {
            Thread t = new Thread(() -> {
                for (int j = 0; j < 1000; j++) {
                    accountingQueue.offer(new EstimableLong(j));
                }

                latch.countDown();
            });
            t.start();
            threads.add(t);
        }

        latch.await();
        assertThat(accountingQueue.getQueue().size(), is(15_000));
        for (Thread thread : threads) {
            thread.join();
        }
    }
}
