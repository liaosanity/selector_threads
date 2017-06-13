package com.wolfit.selector_threads;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ConnectionBody implements Writable {
	
	public static final Log LOG = LogFactory.getLog(ConnectionHeader.class);

	private String body;
	
	public ConnectionBody() {
		
	}
	
	public ConnectionBody(String body) {
		this.body = body;
	}

	public void readFields(DataInput in) throws IOException {
		body = Text.readString(in);
        if (body.isEmpty()) {
        	body = null;
        } else {
        	LOG.info("The body is: " + body);
        }
	}
	
    public void write(DataOutput out) throws IOException {
    	Text.writeString(out, (body == null) ? "" : body);
	}
    
    public String getBody() {
    	return body;
    }
    
    public String toString() {
    	return body;
    }
}
