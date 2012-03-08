package naming;

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
	
	
	public ReplicateThread(Path path, Command replicationTargetCommand, Set<Storage> storageLocations, ConcurrentHashMap<Path, ReadWriteLock> fileLocks, Storage replicationTarget) {
		this.path = path;
		this.replicationTargetCommand = replicationTargetCommand;
		this.storageLocations = storageLocations;
		this.fileLocks = fileLocks;
		this.replicationTarget = replicationTarget;
	}

	
	public void run() {
		boolean result = false;
		try{
			result = replicationTargetCommand.copy(path, getRandomElementFromSet(storageLocations));
		} catch (Exception e){}

		if (result == true){
			storageLocations.add(replicationTarget);
			this.fileLocks.get(path).resetReadCount();
		}
	}

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

