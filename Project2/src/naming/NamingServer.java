package naming;

import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import rmi.RMIException;
import rmi.Skeleton;
import storage.Command;
import storage.Storage;

import common.Path;

/** Naming server.

    <p>
    Each instance of the filesystem is centered on a single naming server. The
    naming server maintains the filesystem directory tree. It does not store any
    file data - this is done by separate storage servers. The primary purpose of
    the naming server is to map each file name (path) to the storage server
    which hosts the file's contents.

    <p>
    The naming server provides two interfaces, <code>Service</code> and
    <code>Registration</code>, which are accessible through RMI. Storage servers
    use the <code>Registration</code> interface to inform the naming server of
    their existence. Clients use the <code>Service</code> interface to perform
    most filesystem operations. The documentation accompanying these interfaces
    provides details on the methods supported.

    <p>
    Stubs for accessing the naming server must typically be created by directly
    specifying the remote network address. To make this possible, the client and
    registration interfaces are available at well-known ports defined in
    <code>NamingStubs</code>.

    <p>
    This version of the naming server supports file locking and file
    replication.

    <p>
    The majority of the naming server logic is implemented in this class.
    Additional logic related to replication and invalidation is implemented in
    <code>FileNode</code>.
 */
public class NamingServer implements Service, Registration
{
    Skeleton<Service> serviceSkeleton;
    Skeleton<Registration> registrationSkeleton;
    ConcurrentHashMap<Path, List<Storage>> storageMap;
    ConcurrentHashMap<Storage, Command> registeredStorageServers;
    ConcurrentHashMap<Path, Set<Path>> directoryStructure;
	
	/** Creates the naming server object.

        <p>
        The naming server is not started.
     */
    public NamingServer()
    {    	
    	this.storageMap = new ConcurrentHashMap<Path, List<Storage>>();
    	this.directoryStructure = new ConcurrentHashMap<Path, Set<Path>>();
    	this.registeredStorageServers = new ConcurrentHashMap<Storage, Command>();
    	
    	InetSocketAddress serviceAddr = new InetSocketAddress(NamingStubs.SERVICE_PORT);
		this.serviceSkeleton = new Skeleton(Service.class, this, serviceAddr);

		InetSocketAddress regAddr = new InetSocketAddress(NamingStubs.REGISTRATION_PORT);
		this.registrationSkeleton = new Skeleton(Registration.class, this, regAddr);
    }

    /** Starts the naming server.

        <p>
        After this method is called, it is possible to access the client and
        registration interfaces of the naming server remotely.

        @throws RMIException If either of the two skeletons, for the client or
                             registration server interfaces, could not be
                             started. The user should not attempt to start the
                             server again if an exception occurs.
     */
    public synchronized void start() throws RMIException
    {
        this.serviceSkeleton.start();
        this.registrationSkeleton.start();
    }

    /** Stops the naming server.

        <p>
        This method waits for both the client and registration interface
        skeletons to stop. It attempts to interrupt as many of the threads that
        are executing naming server code as possible. After this method is
        called, the naming server is no longer accessible remotely. The naming
        server should not be restarted.
     */
    public void stop()
    {
    	this.serviceSkeleton.stop();
    	this.registrationSkeleton.stop();
    	
    	this.stopped(null);
    }

    /** Indicates that the server has completely shut down.

        <p>
        This method should be overridden for error reporting and application
        exit purposes. The default implementation does nothing.

        @param cause The cause for the shutdown, or <code>null</code> if the
                     shutdown was by explicit user request.
     */
    protected void stopped(Throwable cause)
    {
    }

    // The following public methods are documented in Service.java.
    @Override
    public void lock(Path path, boolean exclusive) throws FileNotFoundException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void unlock(Path path, boolean exclusive)
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean isDirectory(Path path) throws FileNotFoundException
    {
        if (path == null){
        	throw new NullPointerException();
        }
        
        boolean isDirectory = this.directoryStructure.containsKey(path);
        boolean isFile = this.storageMap.containsKey(path);
        
        if(!isDirectory && !isFile){
        	throw new FileNotFoundException();
        }
        
        return isDirectory;
        
    }

    @Override
    public String[] list(Path directory) throws FileNotFoundException
    {
    	if (!this.isDirectory(directory)){
    		throw new FileNotFoundException();
    	}
    	
    	Set<Path> pathSet = this.directoryStructure.get(directory);
    	
    	
    	Path[] listing = new Path[pathSet.size()];
    	pathSet.toArray(listing);
    	
    	String[] directoryListing = new String[listing.length];

    	for (int i = 0; i < listing.length; i++){
    		directoryListing[i] = listing[i].getFileName();
    	}
    	
    	
    	return directoryListing;
    	
    }

    @Override
    public boolean createFile(Path file)
        throws RMIException, FileNotFoundException
    {
        if(!file.isRoot() && !this.directoryStructure.containsKey(file.parent())){
        	throw new FileNotFoundException();
        }
        
        if(this.registeredStorageServers.isEmpty()){
        	throw new IllegalStateException();
        }
        
        if (!file.isRoot() && !this.directoryStructure.containsKey(file) && !this.storageMap.containsKey(file)){
        	Collection<Storage> servers = this.registeredStorageServers.keySet();
        	Storage[] storageServers = new Storage[servers.size()];
        	servers.toArray(storageServers);
        	int index  = (int)(storageServers.length*Math.random());
        	Storage chosenStorageStub = storageServers[index];
        	Command chosenCommandStub = this.registeredStorageServers.get(chosenStorageStub);
        	
        	boolean result = chosenCommandStub.create(file);
        	
        	if(result){
        		updateDirectoryStructure(file);
        		ArrayList<Storage> locations = new ArrayList<Storage>();
        		locations.add(chosenStorageStub);
        		this.storageMap.put(file, locations);
        	}
        	
    		return result;
    	}
        
        return false;
    }

    @Override
    public boolean createDirectory(Path directory) throws FileNotFoundException
    {
    	if (!directory.isRoot() && !this.directoryStructure.containsKey(directory.parent())){
    		throw new FileNotFoundException();
    	}
    	
    	if (!directory.isRoot() && !this.directoryStructure.containsKey(directory) && !this.storageMap.containsKey(directory)){
    		updateDirectoryStructure(directory);
    		this.directoryStructure.put(directory, new HashSet<Path>());
    		return true;
    	}
    	
    	return false;
    }

    @Override
    public boolean delete(Path path) throws FileNotFoundException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Storage getStorage(Path file) throws FileNotFoundException
    {
    	if (file == null){
    		throw new NullPointerException();
    	}
    	
    	if (!this.storageMap.containsKey(file)){
    		throw new FileNotFoundException();
    	}
        return this.storageMap.get(file).get(0);
    }

    // The method register is documented in Registration.java.
    @Override
    public Path[] register(Storage client_stub, Command command_stub,
                           Path[] files)
    {
    	if (client_stub == null || command_stub == null || files == null){
    		throw new NullPointerException();
    	} 
    	
    	if (this.registeredStorageServers.containsKey(client_stub)){
    		throw new IllegalStateException();
    	} else {
    		this.registeredStorageServers.put(client_stub, command_stub);
    	}
    	
    	ArrayList<Path> filesToDelete = new ArrayList<Path>();
    	for (Path p : files){
    		if (p.isRoot()){
    			//silently ignore this attempt to add root directory as a file
    		} else if (this.directoryStructure.keySet().contains(p)){
    			filesToDelete.add(p);
    		} else if (this.storageMap.containsKey(p)){
    			filesToDelete.add(p);
    		} else {
    			CopyOnWriteArrayList<Storage> storageList = new CopyOnWriteArrayList<Storage>();
    			storageList.add(client_stub);
    			this.storageMap.put(p, storageList);
    			
    			updateDirectoryStructure(p);
    			
    		}
    	}
    	
    	Path[] dupList = new Path[filesToDelete.size()];
    	filesToDelete.toArray(dupList);

    	return dupList;
    }
    
    private void updateDirectoryStructure(Path p){
    	Path parent = p;
		Path child = p;
		
		while (!child.isRoot()){
			parent = child.parent();
			Set<Path> directoryContents;
			if(this.directoryStructure.containsKey(parent)){
				directoryContents = this.directoryStructure.get(parent);
				directoryContents.add(child);
				this.directoryStructure.put(parent, directoryContents);
			} else {
				directoryContents = Collections.newSetFromMap(new ConcurrentHashMap<Path, Boolean>());
				directoryContents.add(child);
				this.directoryStructure.put(parent, directoryContents);
			}
			child = parent;
		}
    }
}
