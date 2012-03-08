package rmi;

import java.io.*;
import java.lang.reflect.*;
import java.lang.reflect.Proxy;
import java.net.*;

public class RMIInvocationHandler implements InvocationHandler, Serializable {

	private String hostname;
	private int port;
	private InetSocketAddress address;
	private Class intface;
	
	public RMIInvocationHandler(InetSocketAddress address, Class c) {
		this.hostname = address.getHostName();
		this.port = address.getPort();
		this.address = address;
		this.intface = c;
	}
	
	public Object invoke(Object proxy, Method method, Object[] args) throws Exception  {
		
		/* Returns the name of implementing interface and network address 
		 * if local method toString is called */
		if(method.getName().equals("toString") && method.getReturnType().
				getName().equals("java.lang.String") && method.
				getParameterTypes().length == 0) {
			RMIInvocationHandler r = (RMIInvocationHandler) 
					java.lang.reflect.Proxy.getInvocationHandler(proxy);
			
			return r.getintface().getName() + " " + r.getAddress().toString();
		}
		
		/* Returns a hashCode based on implementing interface and network address 
		 * if local method hashCode is called */
		if(method.getName().equals("hashCode") && method.getReturnType().getName()
				.equals("int") && method.getParameterTypes().length == 0) {
			
			RMIInvocationHandler r = (RMIInvocationHandler) 
					java.lang.reflect.Proxy.getInvocationHandler(proxy);
			
			return r.getintface().hashCode() * r.getAddress().hashCode();		
		}
		
		/* Determines if two proxy objects are equal based on the interface 
		 * they implement and address they connect to */
		if(method.getName().equals("equals")&&method.getReturnType().getName().
				equals("boolean") && method.getParameterTypes().length == 1) {
			
			if(args.length != 1 || args[0] == null)
				return false;
			
			RMIInvocationHandler r = (RMIInvocationHandler) 
					java.lang.reflect.Proxy.getInvocationHandler(proxy);
			RMIInvocationHandler q = 
					(RMIInvocationHandler) java.lang.reflect.
					Proxy.getInvocationHandler(args[0]);
			
			if(r.getintface().equals(q.getintface()) 
					&& r.getAddress().equals(q.getAddress()) ) {
				return true;
			}
			else
				return false;		
		}
		
		Socket connection;
		responseObject serverReturn = null;
		
		try {
			/* Connects to server and forwards information and 
			 * Receives a response. Throws an RMIException if 
			 * Problems occurred */
			connection = new Socket(hostname, port);
			ObjectOutputStream out = 
					new ObjectOutputStream(connection.getOutputStream());
			out.flush();
			ObjectInputStream in = 
					new ObjectInputStream(connection.getInputStream());
			/* Sends method, parameter types, return type,
			 * and arguements */
			out.writeObject(method.getName());
			out.writeObject(method.getParameterTypes());
			out.writeObject(method.getReturnType().getName());
			out.writeObject(args); 
			serverReturn = (responseObject) in.readObject();			
			connection.close();
		} catch (IOException e) {
			throw new RMIException(e.getCause());
		} catch (ClassNotFoundException e) {
			throw new RMIException(e.getCause());
		}
			
		/* if the method on the server threw an exception, 
		 * then the local proxy object will too */
		if(serverReturn.isException()) 
			throw (Exception) serverReturn.getReturn();
		
		return serverReturn.getReturn();
	}
	
	/* Helper methods to retrieve private variables */
	public Class getintface() {
		return intface; 
	}
	
	public InetSocketAddress getAddress() {
		return address; 
	}	
}
