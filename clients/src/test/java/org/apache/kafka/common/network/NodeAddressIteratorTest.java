/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.network;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.kafka.common.Node;
import org.junit.Test;

public class NodeAddressIteratorTest {

    @Test
    public void testSingleIP() throws UnknownHostException {
        Node node = new Node(0, "localhost", 9092);
        NodeAddressIterator multi = new NodeAddressIterator(node, 1024, 1024, new InetAddress[] {InetAddress.getLocalHost()});
        assertTrue(multi.isAtFirstAddress());
        assertFalse(multi.hasMoreAddresses());
        try {
            multi.moveToNextAddress();
            fail();
        } catch (IllegalStateException expected) { }
    }

    @Test
    public void testMultipleIPs() throws UnknownHostException {
        Node node = new Node(0, "localhost", 9092);
        NodeAddressIterator multi = new NodeAddressIterator(node, 1024, 1024, new InetAddress[] {InetAddress.getLocalHost(), InetAddress.getLoopbackAddress()});
        assertTrue(multi.isAtFirstAddress());
        assertTrue(multi.hasMoreAddresses());
        multi.moveToNextAddress();
        assertFalse(multi.hasMoreAddresses());
        assertFalse(multi.isAtFirstAddress());
        try {
            multi.moveToNextAddress();
            fail();
        } catch (IllegalStateException expected) { }
    }
    

    // reuse the same funny name in ClientUtilsTest as each new unknown name can take 5s for resolution
    @Test(expected = UnknownHostException.class)
    public void testUnknownHostException() throws UnknownHostException {
        Node node = new Node(0, "some.invalid.hostname.foo.bar.local", 9092);
        new NodeAddressIterator(node, 1024, 1024);
    }

}
