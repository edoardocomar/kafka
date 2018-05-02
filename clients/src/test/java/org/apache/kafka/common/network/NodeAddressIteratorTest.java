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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ClientDnsLookup;
import org.junit.Test;

public class NodeAddressIteratorTest {

    @Test
    public void testSingleIPWithDefault() throws UnknownHostException {
        Node node = new Node(0, "localhost", 9092);
        NodeAddressIterator multi = new NodeAddressIterator(node, 1024, 1024, ClientDnsLookup.DEFAULT, new InetAddress[]{InetAddress.getLocalHost()});
        assertTrue(multi.isAtFirstAddress());
        assertFalse(multi.hasMoreAddresses());
        try {
            multi.moveToNextAddress();
            fail();
        } catch (IllegalStateException expected) { }
    }

    @Test
    public void testSingleIPWithUseAll() throws UnknownHostException {
        Node node = new Node(0, "localhost", 9092);
        NodeAddressIterator multi = new NodeAddressIterator(node, 1024, 1024, ClientDnsLookup.USE_ALL_DNS_IPS, new InetAddress[]{InetAddress.getLocalHost()});
        assertTrue(multi.isAtFirstAddress());
        assertFalse(multi.hasMoreAddresses());
        try {
            multi.moveToNextAddress();
            fail();
        } catch (IllegalStateException expected) { }
    }

    @Test
    public void testMultipleIPsWithUseAll() throws UnknownHostException {
        Node node = new Node(0, "localhost", 9092);
        NodeAddressIterator multi = new NodeAddressIterator(node, 1024, 1024, ClientDnsLookup.USE_ALL_DNS_IPS, new InetAddress[]{InetAddress.getLocalHost(), InetAddress.getLoopbackAddress()});
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

    @Test
    public void testMultipleIPsWithDefault() throws UnknownHostException {
        Node node = new Node(0, "localhost", 9092);
        NodeAddressIterator multi = new NodeAddressIterator(node, 1024, 1024, ClientDnsLookup.DEFAULT, new InetAddress[]{InetAddress.getLocalHost(), InetAddress.getLoopbackAddress()});
        assertTrue(multi.isAtFirstAddress());
        assertFalse(multi.hasMoreAddresses());
        try {
            multi.moveToNextAddress();
            fail();
        } catch (IllegalStateException expected) { }
    }

    // reuse the same funny name in ClientUtilsTest as each new unknown name can take 5s for resolution
    @Test(expected = UnknownHostException.class)
    public void testUnknownHostException() throws UnknownHostException {
        Node node = new Node(0, "some.invalid.hostname.foo.bar.local", 9092);
        new NodeAddressIterator(node, 1024, 1024, ClientDnsLookup.DEFAULT);
    }

    @Test
    public void testDnsLookup() throws UnknownHostException {
        Node node = new Node(0, "localhost", 9092);
        NodeAddressIterator nai = new NodeAddressIterator(node, 1024, 1024, ClientDnsLookup.DEFAULT);
        assertTrue(nai.isAtFirstAddress());
        assertFalse(nai.hasMoreAddresses());
    }

    @Test
    public void testFilterPreferedAddresses() throws UnknownHostException {
        InetAddress ipv4 = InetAddress.getByName("192.0.0.1");
        InetAddress ipv6 = InetAddress.getByName("::1");

        InetAddress[] ipv4First = new InetAddress[]{ipv4, ipv6, ipv4};
        List<InetAddress> result = NodeAddressIterator.filterPreferedAddresses(ipv4First);
        assertTrue(result.contains(ipv4));
        assertFalse(result.contains(ipv6));
        assertEquals(2, result.size());

        InetAddress[] ipv6First = new InetAddress[]{ipv6, ipv4, ipv4};
        result = NodeAddressIterator.filterPreferedAddresses(ipv6First);
        assertTrue(result.contains(ipv6));
        assertFalse(result.contains(ipv4));
        assertEquals(1, result.size());
    }
}
