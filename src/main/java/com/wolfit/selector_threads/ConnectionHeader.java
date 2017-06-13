package com.wolfit.selector_threads;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ConnectionHeader implements Writable {
	
    public static final Log LOG = LogFactory.getLog(ConnectionHeader.class);
    
    private String protocol;
    
    public ConnectionHeader() {
    	
    }
    
    public ConnectionHeader(String protocol) {
    	this.protocol = protocol;
    }

    public void readFields(DataInput in) throws IOException {
    	protocol = Text.readString(in);
        if (protocol.isEmpty()) {
        	protocol = null;
        } else {
        	LOG.info("The protocol is: " + protocol);
        }
    }
    
    public void write(DataOutput out) throws IOException {
    	Text.writeString(out, (protocol == null) ? "" : protocol);
    }
}
