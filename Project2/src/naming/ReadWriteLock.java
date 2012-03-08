package naming;

public class ReadWriteLock {
	  private volatile int readLocksOut = 0;
	  private volatile boolean isWriteLocked = false;
	  private volatile int writeRequests = 0;
	  private volatile int totalReadRequests = 0;
	  private volatile boolean stopped = false; 
	  
	  public synchronized void interrupt() {
		  this.stopped = true;
		  this.notifyAll();
	  }

	  public synchronized void lockRead() throws InterruptedException {
		  while(!stopped && (isWriteLocked || writeRequests > 0)){
			  wait();
		  }
		  readLocksOut++;
		  totalReadRequests++;
	  }

	  public synchronized void unlockRead() {
		readLocksOut--;
		notifyAll();
	  }

	  public synchronized void lockWrite() throws InterruptedException {
	    writeRequests++;

	    while(!stopped && (readLocksOut > 0 || isWriteLocked)){
	      wait();
	    }
	    writeRequests--;
	    isWriteLocked = true;
	  }

	  public synchronized void unlockWrite() throws InterruptedException {
		isWriteLocked = false;
	    notifyAll();
	  }
	  
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
