package rmi;

import java.io.IOException;
import java.net.*;

public class listenThread extends Thread {
	
	private Skeleton skeleton = null;
	private ServerSocket ssocket = null;
	private Exception stopCause = null;
	
	public listenThread(Skeleton skeleton, ServerSocket listen_socket) {
		
		this.skeleton = skeleton;
		this.ssocket = listen_socket;
	}
	
	public void run()  {
			
		/* Listening thread loops which server is running
		 * and accepts connections */
		while(skeleton.isRunning() && !this.isInterrupted()) {
						
			try {
				Socket connection = ssocket.accept();
				
				/* Accepts connection and handles request in a separate 
				 * thread */
				dispatchThread dispatch = 
						new dispatchThread(connection, skeleton);		
				if(skeleton.isRunning())
					dispatch.start();			
							
			} catch (IOException e) {
				/* Handles exceptions based on return of listen error */
				if(skeleton.isRunning() && skeleton.listen_error(e)) {
					//do nothing 
				}
				else {
					/* Shut's the server down */
					skeleton.setisRunning(false);
					this.interrupt();
					this.stopCause = e;
				}
			}						
		}
					
		/* stopped method is called when the listening thread exits */
		skeleton.stopped(stopCause);			
	}
	
}
