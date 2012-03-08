package naming;

public class ReadWriteLock {
	  private volatile int readLocksOut = 0;
	  private volatile boolean isWriteLocked = false;
	  private volatile int writeRequests = 0;
	  private volatile int totalReadRequests = 0;
	  private volatile boolean isStopped = false; 
	  
	  public synchronized void interrupt() {
		  this.isStopped = true;
		  this.notifyAll();
	  }

	  public synchronized void lockRead() throws InterruptedException {
		  while(!isStopped && (isWriteLocked || writeRequests > 0)){
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

	    while(!isStopped && (readLocksOut > 0 || isWriteLocked)){
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
	  
}
