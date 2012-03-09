package naming;

/******************************************************************************
 * 
 * Authors: Christopher Tomaszewski (CKT) & Dinesh Palanisamy (DINESHP) 
 * 
 ******************************************************************************/

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import storage.Command;
import storage.Storage;
import common.Path;

public class ReplicateThread implements Runnable {

	private Path path;
	private Command replicationTargetCommand;
	private Set<Storage> storageLocations;
	private ConcurrentHashMap<Path, ReadWriteLock> fileLocks; 
	private Storage replicationTarget;
	
	/* Initializes objects needed to replicate and update data structures */
	public ReplicateThread(Path path, Command replicationTargetCommand, Set<Storage> 
	storageLocations, ConcurrentHashMap<Path, ReadWriteLock> fileLocks, 
	Storage replicationTarget) {
		this.path = path;
		this.replicationTargetCommand = replicationTargetCommand;
		this.storageLocations = storageLocations;
		this.fileLocks = fileLocks;
		this.replicationTarget = replicationTarget;
	}

	
	/* Calls copy on a command stub of a server and updates the hashmap of path 
	 * to storage stubs by adding new storage stub */
	public void run() {
		boolean result = false;
		try{
			/* Copies to target server given a storage stub containing a copy */
			result = replicationTargetCommand.copy(path, 
					getRandomElementFromSet(storageLocations));
		} catch (Exception e){}

		if (result == true){
			/* Adds new storage stub to the path to storage stubs HashMap */
			storageLocations.add(replicationTarget);
			this.fileLocks.get(path).resetReadCount();
		}
	}

    /* Helper method that returns a random element from a given set */
	private <T> T getRandomElementFromSet(Set<T> set){
    	if(set.isEmpty()){
    		return null;
    	}
    	
    	int index = (int)(set.size() * Math.random()); 
    	
    	Object[] array = new Object[set.size()];
    	set.toArray(array);
    	
    	return (T)array[index];
    }


}

