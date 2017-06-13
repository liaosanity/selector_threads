package com.wolfit.selector_threads;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    private Socket socket;

	/**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );

//        try {
//        	while (true) {
//        		try {
//        			socket = new Socket();
//                	SocketAddress remoteAddr = new InetSocketAddress("127.0.0.1", 8090);
//                	socket.connect(remoteAddr, 60000);
//                	socket.setTcpNoDelay(false);
//        			break;
//        		} catch (IOException ie) {
//        		    if (socket != null) {
//        		        try {
//        		            socket.close();
//        		        } catch (IOException e) {
//        		        	e.printStackTrace();
//        		        }
//        		    }
//        			
//        		    socket = null;
//        		    
//        		    try {
//        		      Thread.sleep(1000);
//        		    } catch (InterruptedException ignored) {
//        		   	  
//        		    }
//        		}
//        	}
//            
//            DataInputStream in = new DataInputStream(
//            		new BufferedInputStream(socket.getInputStream()));
//            DataOutputStream out = new DataOutputStream(
//            		new BufferedOutputStream(socket.getOutputStream()));
//            
//            DataOutputBuffer buf = new DataOutputBuffer();
//            ConnectionHeader header = new ConnectionHeader("TLV");
//            header.write(buf);
//            
//            int bufLen = buf.getLength();
//            out.writeInt(bufLen);
//            out.write(buf.getData(), 0, bufLen);
//            out.flush();
//            
//            DataOutputBuffer buf2 = new DataOutputBuffer();
//            ConnectionBody wbody = new ConnectionBody("are you ok!!!");
//            wbody.write(buf2);
//            
//            bufLen = buf2.getLength();
//            out.writeInt(bufLen);
//            out.write(buf2.getData(), 0, bufLen);
//            out.flush();
//            
//            ConnectionBody rbody = new ConnectionBody();
//            rbody.readFields(in);
//	
//			out.close();
//			in.close();
//			
//			socket.close();
//			
//			System.out.println("test over!!!");
//		} catch (UnknownHostException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
    }
}
