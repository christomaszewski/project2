package storage;

import java.io.*;
import java.net.*;
import java.util.Arrays;

import common.*;
import rmi.*;
import naming.*;

/** Storage server.

    <p>
    Storage servers respond to client file access requests. The files accessible
    through a storage server are those accessible under a given directory of the
    local filesystem.
 */
public class StorageServer implements Storage, Command
{
    private Skeleton storageSkeleton;
    private Skeleton commandSkeleton;
    private File root;
    private boolean ioExceptionThrown = false;

	/** Creates a storage server, given a directory on the local filesystem, and
        ports to use for the client and command interfaces.

        <p>
        The ports may have to be specified if the storage server is running
        behind a firewall, and specific ports are open.

        @param root Directory on the local filesystem. The contents of this
                    directory will be accessible through the storage server.
        @param client_port Port to use for the client interface, or zero if the
                           system should decide the port.
        @param command_port Port to use for the command interface, or zero if
                            the system should decide the port.
        @throws NullPointerException If <code>root</code> is <code>null</code>.
    */
    public StorageServer(File root, int client_port, int command_port)
    {
        if (root == null){
        	throw new NullPointerException();
        }
        this.root = root;
        InetSocketAddress storageAddr = new InetSocketAddress(client_port);
        this.storageSkeleton = new Skeleton(Storage.class, this, storageAddr);
        
        InetSocketAddress commandAddr = new InetSocketAddress(command_port);
        this.commandSkeleton = new Skeleton(Command.class, this, commandAddr);
    }

    /** Creats a storage server, given a directory on the local filesystem.

        <p>
        This constructor is equivalent to
        <code>StorageServer(root, 0, 0)</code>. The system picks the ports on
        which the interfaces are made available.

        @param root Directory on the local filesystem. The contents of this
                    directory will be accessible through the storage server.
        @throws NullPointerException If <code>root</code> is <code>null</code>.
     */
    public StorageServer(File root)
    {
        this(root, 0, 0);
    }

    /** Starts the storage server and registers it with the given naming
        server.

        @param hostname The externally-routable hostname of the local host on
                        which the storage server is running. This is used to
                        ensure that the stub which is provided to the naming
                        server by the <code>start</code> method carries the
                        externally visible hostname or address of this storage
                        server.
        @param naming_server Remote interface for the naming server with which
                             the storage server is to register.
        @throws UnknownHostException If a stub cannot be created for the storage
                                     server because a valid address has not been
                                     assigned.
        @throws FileNotFoundException If the directory with which the server was
                                      created does not exist or is in fact a
                                      file.
        @throws RMIException If the storage server cannot be started, or if it
                             cannot be registered.
     */
    public synchronized void start(String hostname, Registration naming_server)
        throws RMIException, UnknownHostException, FileNotFoundException
    {
    	this.commandSkeleton.start();
    	this.storageSkeleton.start();
    	
        Storage storageStub = Stub.create(Storage.class, this.storageSkeleton, hostname);
        
        Command commandStub = Stub.create(Command.class, this.commandSkeleton, hostname);
        
    	Path[] dupList = naming_server.register(storageStub, commandStub, Path.list(this.root));
    	
    	for (Path p : dupList){
    		this.delete(p);
    	}
    	
    	pruneDirectories(this.root);
    	
    	
    }
    
    private boolean pruneDirectories(File node){
    	boolean safeToDelete = true;
    	
    	File[] directoryListing = node.listFiles();
    	
    	for (File f : directoryListing){
    		if (f.isDirectory()){
    			safeToDelete = safeToDelete && pruneDirectories(f);
    		} else {
    			safeToDelete = false;
    		}
    	}
    	
    	if (safeToDelete){
    		node.delete();
    	}
    	
    	return safeToDelete;
    }

    /** Stops the storage server.

        <p>
        The server should not be restarted.
     */
    public void stop()
    {
    	this.commandSkeleton.stop();
    	this.storageSkeleton.stop();
    	
    	this.stopped(null);
    }

    /** Called when the storage server has shut down.

        @param cause The cause for the shutdown, if any, or <code>null</code> if
                     the server was shut down by the user's request.
     */
    protected void stopped(Throwable cause)
    {
    }

    // The following methods are documented in Storage.java.
    @Override
    public synchronized long size(Path file) throws FileNotFoundException
    {
    	File f = file.toFile(this.root);
    	
    	if (!f.exists() || f.isDirectory()){
    		throw new FileNotFoundException();
    	}
    	
    	long size = f.length();
    	return size;
    }

    @Override
    public synchronized byte[] read(Path file, long offset, int length)
        throws FileNotFoundException, IOException
    {
    	File f = file.toFile(this.root);
    	
    	if (!f.exists() || f.isDirectory()){
    		throw new FileNotFoundException();
    	}
    	
    	if(offset + length > f.length() || offset < 0 || length < 0) {
    		throw new IndexOutOfBoundsException();
    	}
    	
    	
    	FileInputStream reader = new FileInputStream(f);
    	byte[] data = new byte[length];
    	reader.read(data, (int)offset, length);
    	
    	return data;
    }

    @Override
    public synchronized void write(Path file, long offset, byte[] data)
        throws FileNotFoundException, IOException
    {
    	File f = file.toFile(this.root);
    	
    	if (!f.exists() || f.isDirectory()){
    		throw new FileNotFoundException();
    	}
    	
    	if(offset > f.length()) {
    		FileOutputStream writer = new FileOutputStream(f, true);
    		while (f.length() < offset){
    			writer.write(0);
    		}
        	writer.write(data);
    	} else {
    		FileOutputStream writer = new FileOutputStream(f);
        	writer.write(data, (int)offset, data.length);
    	}
    	
    }

    // The following methods are documented in Command.java.
    @Override
    public synchronized boolean create(Path file)
    {    	
    	if (file.isRoot()){
    		return false;
    	}
    	
        File f = file.toFile(this.root);
        
        Path parent = file.parent();
        if (!parent.toFile(this.root).exists()){
        	parent.toFile(this.root).mkdirs();
        } 
        
        try {
			return f.createNewFile();
		} catch (IOException e) {
			this.ioExceptionThrown = true;
		}
        
        return false;
    }

    @Override
    public synchronized boolean delete(Path path)
    {
    	if (path.isRoot()){
    		return false;
    	}
    	
    	return this.delete(path.toFile(this.root));
    }

    private synchronized boolean delete(File file){
    	boolean allFilesDeleted = true;
    	if (file.isDirectory()){
    		for (File f : file.listFiles()){
    			allFilesDeleted = allFilesDeleted && this.delete(f);
    		}
    		if (allFilesDeleted){
    			file.delete();
    		}
    	} else {
    		allFilesDeleted = file.delete();
    	}
    	
    	return allFilesDeleted;
    }
    
    @Override
    public synchronized boolean copy(Path file, Storage server)
        throws RMIException, FileNotFoundException, IOException
    {
    	long size = server.size(file);
    	byte[] data = server.read(file, 0, (int) size);
    	
    	this.ioExceptionThrown = false;
    	this.create(file);
    	if(this.ioExceptionThrown){
    		throw new IOException();
    	}
    	
    	this.write(file, 0, data);
    	
    	byte[] localData = this.read(file, 0, (int)size);
    	
    	return Arrays.equals(data, localData);
    }
}