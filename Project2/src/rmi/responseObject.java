/* Wrapper class for marking if the server method threw an exception or not */
/* Serializable to able to send across a connection */

package rmi;

import java.io.Serializable;

public class responseObject implements Serializable {
	
	private Object serverReturn = null;
	private boolean isException = false;
	
	public responseObject(boolean isException, Object serverReturn) {
		this.isException = isException;
		this.serverReturn = serverReturn;
	}
	
	public Object getReturn() {
		return serverReturn;
	}
	
	public boolean isException() {
		return isException; 
	}
	
}
