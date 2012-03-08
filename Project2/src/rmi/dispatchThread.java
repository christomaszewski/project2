package rmi;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;

public class dispatchThread extends Thread {

	private Socket connection;
	private Skeleton skeleton;
	
	public dispatchThread(Socket connection, Skeleton skeleton) {
		this.connection = connection;
		this.skeleton = skeleton;
	}

	public void run() {
		ObjectOutputStream out = null;
		ObjectInputStream in = null;
		/* wrapper class to denote if the response is an exception or not */
		responseObject response = null;
	
			try {
				/* create input and output streams */
				out = new ObjectOutputStream(connection.getOutputStream());
				out.flush();
				in = new ObjectInputStream(connection.getInputStream());
				
				/* read in method name, parameter types, return type and 
				 * arguments */
				Object methodName = in.readObject();	
				Object parameterTypes = in.readObject();
				Object returnType = in.readObject();
				Object args = in.readObject();
				
				/* retrieve the proper Method from the given interface */
				Method serverMethod = skeleton.getIntface().
						getMethod((String)methodName,(Class[])parameterTypes);

				/* throw an exception if the return types don't match */
				if(returnType.equals(serverMethod.
						getReturnType().getName()) == false ) {	
					Throwable t = new RMIException("Return Type Mismatch");
					response = new responseObject(true, t);
				}
					
				/* call the proper method on the server */
				try {
					Object serverReturn = serverMethod.
							invoke(skeleton.getServer(), (Object [])args);
					response = new responseObject(false, serverReturn);
					/* response in not an exception */
				} catch(IllegalAccessException e){
					/* response is an exception */
					Throwable t = new RMIException(e.getCause());
					response = new responseObject(true, t);
				} catch(IllegalArgumentException e) {
					/* response is an exception */
					Throwable t = new RMIException(e.getCause());
					response = new responseObject(true, t);
				} catch(InvocationTargetException e) {
					/* Underlying method threw an exception */
					response = new responseObject(true, e.getCause());
					out.writeObject(response);
				}
					
				/* send the return value of the method in a wrapper */
				out.writeObject(response);
				connection.close();
			} catch (ClassNotFoundException e){
				/* error occurred in the service thread */
				skeleton.service_error(new RMIException(e.getCause()));
			} catch (NoSuchMethodException e) {
				/* error occurred in the service thread */
				skeleton.service_error(new RMIException(e.getCause()));
			} catch (SecurityException e) {
				/* error occurred in the service thread */
				skeleton.service_error(new RMIException(e.getCause()));
			} catch(IOException e) {
				/* error occurred in the service thread */
				skeleton.service_error(new RMIException(e.getCause()));
			}
	}
}
