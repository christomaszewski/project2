package naming;

/******************************************************************************
 * 
 * Authors: Christopher Tomaszewski (CKT) & Dinesh Palanisamy (DINESHP) 
 * 
 ******************************************************************************/

public class ReadWriteLock {
	private volatile int readLocksOut = 0;
	private volatile boolean isWriteLocked = false;
	private volatile int writeRequests = 0;
	private volatile int totalReadRequests = 0;
	private volatile boolean stopped = false; 
	  
	/* Flips interrupt flag and notifies all */ 
	public synchronized void interrupt() {
		this.stopped = true;
		this.notifyAll();
	}

	/* Read lock blocks while writers are out and if a write request is waiting */
	public synchronized void lockRead() throws InterruptedException {	
		while(!stopped && (isWriteLocked || writeRequests > 0)){
			wait();
		}
		/* Update request and reader count */
		readLocksOut++;
		totalReadRequests++;
	}

	/* Update reader count and notify blocking threads */
	public synchronized void unlockRead() {
		readLocksOut--;
		notifyAll();
	}

	
	public synchronized void lockWrite() throws InterruptedException {
		/* Update request counter */
		writeRequests++;

		/* Block while readers and writers are out */
		while(!stopped && (readLocksOut > 0 || isWriteLocked)){
			wait();
		}
		/* Update count and boolean when lock is taken */
		writeRequests--;
	    isWriteLocked = true;
	}

	public synchronized void unlockWrite() throws InterruptedException {
		/* Update writer boolean and notify */
		isWriteLocked = false;
	    notifyAll();
	}
	  
	/* Methods to check status and get counts in lock */
	public synchronized boolean isWriteLocked() {
		return isWriteLocked;
	}
	  
	public synchronized boolean isReadLocked() {
		return readLocksOut > 0;
	}
	  
	public synchronized boolean hasWriteRequests() {
		return writeRequests > 0;
	}
	  
	public synchronized int getTotalReadRequests() {
		return this.totalReadRequests;
	}
	  
	public synchronized void resetReadCount(){
		totalReadRequests = 0;
	}
	  
	public synchronized boolean isStopped(){
		return this.stopped;
	}
	
}
