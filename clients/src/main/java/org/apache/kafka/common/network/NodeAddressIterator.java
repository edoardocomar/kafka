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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import org.apache.kafka.common.Node;

/**
 * A mutable object for storing the state required to connect to Nodes that
 * resolve to multiple IP addresses
 *
 */
class NodeAddressIterator {

    private final Node node;
    private final InetAddress[] addresses;
    private final int sendBufferSize;
    private final int receiveBufferSize;
    // Mutable state
    private int index = 0;

    public NodeAddressIterator(Node node, int sendBufferSize, int rcvBufferSize) throws UnknownHostException {
        this(node, sendBufferSize, rcvBufferSize, resolve(node));
    }

    // constructor for testing
    NodeAddressIterator(Node node, int sendBufferSize, int rcvBufferSize, InetAddress[] addresses) throws UnknownHostException {
        this.node = node;
        this.addresses = addresses;
        this.sendBufferSize = sendBufferSize;
        this.receiveBufferSize = rcvBufferSize;
    }

    public Node node() {
        return node;
    }

    public String id() {
        return node.idString();
    }

    public int sendBufferSize() {
        return sendBufferSize;
    }

    public int receiveBufferSize() {
        return receiveBufferSize;
    }

    public boolean isAtFirstAddress() {
        return index == 0;
    }

    public InetAddress currAddress() {
        if (index >= addresses.length) {
            throw new IllegalStateException("No more addresses available");
        }
        return addresses[index];
    }

    public boolean hasMoreAddresses() {
        return index < addresses.length - 1;
    }

    public void moveToNextAddress() {
        if (!hasMoreAddresses()) {
            throw new IllegalStateException("No more addresses available");
        }
        index++;
    }

    static InetAddress[] resolve(Node node) throws UnknownHostException {
        return InetAddress.getAllByName(node.host());
    }

    @Override
    public String toString() {
        return "NodeAddressIterator [node=" + node + ", addresses=" + Arrays.toString(addresses) + ", sendBufferSize="
                + sendBufferSize + ", receiveBufferSize=" + receiveBufferSize + ", index=" + index + "]";
    }

}
