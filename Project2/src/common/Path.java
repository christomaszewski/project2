package common;

/******************************************************************************
 * 
 * Authors: Christopher Tomaszewski (CKT) & Dinesh Palanisamy (DINESHP) 
 * 
 ******************************************************************************/

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.concurrent.CopyOnWriteArrayList;

/** Distributed filesystem paths.

    <p>
    Objects of type <code>Path</code> are used by all filesystem interfaces.
    Path objects are immutable.

    <p>
    The string representation of paths is a forward-slash-delimeted sequence of
    path components. The root directory is represented as a single forward
    slash.

    <p>
    The colon (<code>:</code>) and forward slash (<code>/</code>) characters are
    not permitted within path components. The forward slash is the delimeter,
    and the colon is reserved as a delimeter for application use.
 */
public class Path implements Iterable<String>, Comparable<Path>, Serializable
{
    private CopyOnWriteArrayList<String> components;

	/** Creates a new path which represents the root directory. */
    public Path()
    {
        this.components = new CopyOnWriteArrayList<String>();
    }

    /** Creates a new path by appending the given component to an existing path.

        @param path The existing path.
        @param component The new component.
        @throws IllegalArgumentException If <code>component</code> includes the
                                         separator, a colon, or
                                         <code>component</code> is the empty
                                         string.
    */
    public Path(Path path, String component)
    {
    	this(path.toString());
    	
    	if (component.equals("")||
    		component.contains(":")||
    		component.contains("/")){
    		
    		throw new IllegalArgumentException();
    	}
    	this.components.add(component);
    }

    /** Creates a new path from a path string.

        <p>
        The string is a sequence of components delimited with forward slashes.
        Empty components are dropped. The string must begin with a forward
        slash.

        @param path The path string.
        @throws IllegalArgumentException If the path string does not begin with
                                         a forward slash, or if the path
                                         contains a colon character.
     */
    public Path(String path)
    {
    	if (!path.startsWith("/") || path.contains(":")){
    		throw new IllegalArgumentException();
    	}
    	
    	this.components = new CopyOnWriteArrayList<String>();
    	
    	//Parse string into path components
        StringTokenizer strtok = new StringTokenizer(path, "/");
        while(strtok.hasMoreTokens()){
        	this.components.add(strtok.nextToken());
        }
    }

    /** Returns an iterator over the components of the path.

        <p>
        The iterator cannot be used to modify the path object - the
        <code>remove</code> method is not supported.

        @return The iterator.
     */
    @Override
    public Iterator<String> iterator()
    {
        PathIterator p = new PathIterator(this.components.iterator());
        return p;
    }

    /** Lists the paths of all files in a directory tree on the local
        filesystem.

        @param directory The root directory of the directory tree.
        @return An array of relative paths, one for each file in the directory
                tree.
        @throws FileNotFoundException If the root directory does not exist.
        @throws IllegalArgumentException If <code>directory</code> exists but
                                         does not refer to a directory.
     */
    public static Path[] list(File directory) throws FileNotFoundException
    {
        if(!directory.exists()){
        	throw new FileNotFoundException();
        } else if (!directory.isDirectory()){
        	throw new IllegalArgumentException();
        }
        
        
        
        ArrayList<Path> pathList = new ArrayList<Path>();
        
        for (File f : directory.listFiles()){
        	
        	//If f is a directory, recursively list its contents
        	if (f.isDirectory()){
        		Path[] directoryListing = Path.list(f);
        		for (Path p : directoryListing){
        			pathList.add(new Path("/" + f.getName() + p.toString()));
        		}
        	} else {
        		pathList.add(new Path("/" + f.getName()));
        	}
        }
        
        Path[] pathArray = new Path[pathList.size()];
        pathList.toArray(pathArray);
        return pathArray;
    }

    /** Determines whether the path represents the root directory.

        @return <code>true</code> if the path does represent the root directory,
                and <code>false</code> if it does not.
     */
    public boolean isRoot()
    {
    	return this.components.isEmpty();
    }

    /** Returns the path to the parent of this path.

        @throws IllegalArgumentException If the path represents the root
                                         directory, and therefore has no parent.
     */
    public Path parent()
    {
    	if (this.isRoot()){
    		throw new IllegalArgumentException();
    	} else {
    		String last = this.components.remove(this.components.size()-1);
    		String parentPath = this.toString();
    		this.components.add(last);
    		return new Path(parentPath);
    	}
    }

    /** Returns the last component in the path.

        @throws IllegalArgumentException If the path represents the root
                                         directory, and therefore has no last
                                         component.
     */
    public String last()
    {
    	if (this.isRoot()){
    		throw new IllegalArgumentException();
    	} else {
    		return this.components.get(this.components.size()-1);
    	}
    }

    /** Determines if the given path is a subpath of this path.

        <p>
        The other path is a subpath of this path if it is a prefix of this path.
        Note that by this definition, each path is a subpath of itself.

        @param other The path to be tested.
        @return <code>true</code> If and only if the other path is a subpath of
                this path.
     */
    public boolean isSubpath(Path other)
    {
    	if (other.isRoot()){
    		return true;
    	}
    	
    	int localPathLength = this.components.size();
    	int otherPathLength = other.getNumberOfComponents();
    	Path localPath = this;
    	
    	//Stop when localPath has less components than other
    	while(localPathLength >= otherPathLength){
    		if (other.equals(localPath)){
    			return true;
    		} else {
    			localPath = localPath.parent();
    			localPathLength--;
    		}
    	}
    	return false;
    }

    /** Converts the path to <code>File</code> object.

        @param root The resulting <code>File</code> object is created relative
                    to this directory.
        @return The <code>File</code> object.
     */
    public File toFile(File root)
    {
        return new File(root, this.toString());
    }

    /** Compares this path to another.

        <p>
        An ordering upon <code>Path</code> objects is provided to prevent
        deadlocks between applications that need to lock multiple filesystem
        objects simultaneously. By convention, paths that need to be locked
        simultaneously are locked in increasing order.

        <p>
        Because locking a path requires locking every component along the path,
        the order is not arbitrary. For example, suppose the paths were ordered
        first by length, so that <code>/etc</code> precedes
        <code>/bin/cat</code>, which precedes <code>/etc/dfs/conf.txt</code>.

        <p>
        Now, suppose two users are running two applications, such as two
        instances of <code>cp</code>. One needs to work with <code>/etc</code>
        and <code>/bin/cat</code>, and the other with <code>/bin/cat</code> and
        <code>/etc/dfs/conf.txt</code>.

        <p>
        Then, if both applications follow the convention and lock paths in
        increasing order, the following situation can occur: the first
        application locks <code>/etc</code>. The second application locks
        <code>/bin/cat</code>. The first application tries to lock
        <code>/bin/cat</code> also, but gets blocked because the second
        application holds the lock. Now, the second application tries to lock
        <code>/etc/dfs/conf.txt</code>, and also gets blocked, because it would
        need to acquire the lock for <code>/etc</code> to do so. The two
        applications are now deadlocked.

        @param other The other path.
        @return Zero if the two paths are equal, a negative number if this path
                precedes the other path, or a positive number if this path
                follows the other path.
     */
    @Override
    public int compareTo(Path other)
    {
    	//In our locking scheme we lock downwards from the root directory
    	//Therefore, we want to ensure files toward the top of the directory,
    	//structure, i.e. the root node, get locked first. We can ensure this
    	//by ordering Paths by the number of components they have. The root node
    	//has the highest locking priority and accordingly, it is the only path
    	//with 0 components. Paths with the same number of components point
    	//to files at the same level in the directory tree and so they do not 
    	//depend on one another and can be locked alphabetically.
    	
    	int otherSize = other.getNumberOfComponents();
    	
    	if(this.components.size() != otherSize){
    		//paths on different levels, use number of components
    		return this.components.size()-otherSize;
    	} else {
    		//paths on same level of directory tree so use alphabetical ordering
    		return this.toString().compareTo(other.toString());
    	}
    }

    public int getNumberOfComponents(){
    	return this.components.size();
    }
    
    /** Compares two paths for equality.

        <p>
        Two paths are equal if they share all the same components.

        @param other The other path.
        @return <code>true</code> if and only if the two paths are equal.
     */
    @Override
    public boolean equals(Object other)
    {
        return this.toString().equals(other.toString());
    }

    /** Returns the hash code of the path. */
    @Override
    public int hashCode()
    {
    	return this.toString().hashCode();
    }

    /** Converts the path to a string.

        <p>
        The string may later be used as an argument to the
        <code>Path(String)</code> constructor.

        @return The string representation of the path.
     */
    @Override
    public String toString()
    {
    	if (this.components.isEmpty()){
    		return "/";
    	}
    	
    	StringBuilder sb = new StringBuilder();
    	for(String s : this.components){
    		sb.append("/");
    		sb.append(s);
    	}
        
    	return sb.toString();
    }
    
    public String getFileName(){
    	//return last component
    	return this.components.get(this.components.size()-1);
    }
    
    //return a list of all possible subpaths for locking purposes
    public Path[] getSubPaths() {
    		
    	if (this.isRoot()){
    		Path[] listing = new Path[1];
    		listing[0] = this;
    		return listing;
    	}
    	else {
    	
	    	Path[] listing = new Path[components.size() + 1];
	    	listing[0] = new Path();
	    	
	    	StringBuilder sb = new StringBuilder();
	    	for(int i = 0; i < components.size(); i++) {
	    		sb.append("/");
	    		sb.append(components.get(i));
	    		listing[i + 1] = new Path(sb.toString()); 
	    	}
	    	
	    	
	    	return listing;
    	}
    	
    }
    
    
}