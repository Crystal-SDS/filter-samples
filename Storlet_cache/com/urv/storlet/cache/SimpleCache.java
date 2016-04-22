package com.urv.storlet.cache;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

public class SimpleCache {
	
	//Eviction policies supported
	public static final String LRU = "LRU";
	public static final String LFU = "LFU";

    //Cache size limit in bytes
    long cacheSize = 0;	  
    //Cache eviction policy
    String policy = LRU;
    TreeSet<CacheObjectDescriptor> sortedDescriptors = null;
    //Fast access to cache object descriptors sorted
    HashMap<String, CacheObjectDescriptor> descriptorsMap = new HashMap<>();
    int getHits = 0;
	int putHits = 0;
	int misses = 0;
	int evictions = 0;
	int reads = 0;
	int writes = 0;
	long cacheMaxSize = 0;
    
    public SimpleCache(long cacheMaxSize, String policy){
	    //Cache eviction policy
	    this.policy = policy;
	    this.cacheMaxSize = cacheMaxSize;
	    //Instantiate eviction policy
	    if (policy.equals(LRU)){
	    	this.sortedDescriptors = new TreeSet<>(new LeastRecentlyUsed());
	    } else if (policy.equals(LFU)) {
	    	this.sortedDescriptors = new TreeSet<>(new LeastFrequentlyUsed());
	    } else {
	    	System.err.println("Incorrect eviction policy! Going to default (LRU)");
	    	this.sortedDescriptors = new TreeSet<>(new LeastRecentlyUsed());
	    }
    }
    
    public synchronized List<String> accessCache(String operation, String blockId, long blockSize){
    	List<String> result = null;	        
        if (operation.equals("PUT")){
        	result = this.put(blockId, blockSize);
        } else if (operation.equals("GET")){
        	result = this.get(blockId);
        } else System.err.println("Unsupported cache operation" + operation);
        return result;
    }
                
    private List<String> put(String blockId, long blockSize){
        this.writes+=1;
        List<String> toEvictId = null;
        //Check if the cache is full and if the element is new
        if ((this.cacheMaxSize <= (this.cacheSize+blockSize)) && !this.descriptorsMap.containsKey(blockId)){
            //Evict as many files as necessary until having enough space for new one
        	toEvictId = new ArrayList<>();
        	while (this.cacheMaxSize <= (this.cacheSize+blockSize)){
        		//Get the last element ordered by the eviction policy
	        	CacheObjectDescriptor toEvict = this.sortedDescriptors.pollLast();
	        	//Remove this entry from the map
	            this.descriptorsMap.remove(toEvict.objectId);
	            //Reduce the size of the cache
	            this.cacheSize -= toEvict.size;
	            this.evictions+=1;
	            toEvictId.add(toEvict.objectId);
        	}
        }
        //If the object is already stored, update its put statistics
        if(this.descriptorsMap.containsKey(blockId)){
        	CacheObjectDescriptor descriptor = this.descriptorsMap.get(blockId);
            this.sortedDescriptors.remove(descriptor);
            descriptor.putHit();
            this.sortedDescriptors.add(descriptor);
            this.putHits += 1;    
        //If not, just add it to the cache
        }else{
            //Add the new element to the cache
        	CacheObjectDescriptor descriptor = new CacheObjectDescriptor(blockId, blockSize);
            this.sortedDescriptors.add(descriptor);
            this.descriptorsMap.put(blockId, descriptor);
            this.cacheSize += blockSize;
        }
        assert this.sortedDescriptors.size() == this.descriptorsMap.size(): 
        	"Unequal length in cache data structures";
        //This object id should be deleted from the cache store
        return toEvictId;
    }
        
    private List<String> get(String blockId){	    	
    	this.reads+=1;
    	CacheObjectDescriptor descriptor = descriptorsMap.get(blockId);
        if (descriptor!=null){ 
        	this.sortedDescriptors.remove(descriptor);
            descriptor.getHit();      
            this.sortedDescriptors.add(descriptor);
            this.getHits += 1;    
            List<String> result = new ArrayList<>();
            result.add(blockId);
            return result;
        }
        this.misses+=1;
        return null;
    }
        
    public String toString() {
    	String cacheElements = "\nCACHE ELEMENTS \t " + this.sortedDescriptors.size() + "\n";
    	//Iterator<CacheObjectDescriptor> iterator = this.sortedDescriptors.descendingIterator();
    	for (CacheObjectDescriptor c: this.sortedDescriptors) {
    		cacheElements += c.objectId + "\t" + c.getHits + "\t" + c.putHits + "\t" +  c.numAccesses + "\t" + c.lastAccess + "\n";
        }
    	return "CACHE GET HITS: " + this.getHits + "\n" +
    			"CACHE PUT HITS: " + this.putHits + "\n" +
    			"CACHE MISSES: " + this.misses + "\n" +
    			"CACHE EVICTIONS: " + this.evictions + "\n" +
    			"CACHE READS: " + this.reads + "\n" +
    			"CACHE WRITES: " + this.writes + "\n" +
    			"CACHE SIZE: " + this.cacheSize + cacheElements;
    	
    }
}

class CacheObjectDescriptor {
	
	 String objectId = null;
     long lastAccess = 0;
     int getHits = 0;
     int putHits = 0;
     int numAccesses = 0;
     long size = 0;
    
    CacheObjectDescriptor(String objectId, long size){
    	this.objectId = objectId;
    	this.size = size;
    	this.lastAccess = System.currentTimeMillis();
	}
        
    void getHit(){
        this.getHits += 1;
        this.hit();
    }
    
    void putHit(){
        this.putHits += 1;
        this.hit();
    }
        
    void hit(){        
    	this.lastAccess = System.currentTimeMillis();
    	this.numAccesses += 1;
    }
    
    /**
     * Two descriptors are equal if they represent the same object_id
     */
    @Override
    public boolean equals(Object obj) {
    	CacheObjectDescriptor toCompare = null;
    	if (obj instanceof CacheObjectDescriptor) {
			toCompare = (CacheObjectDescriptor) obj;			
		}else return false;    	
    	return this.objectId.equals(toCompare.objectId);
    }
    @Override
    public int hashCode() {
    	return this.objectId.hashCode();
    }        
}

// EVICION POLICIES AS COMPARATORS --

class LeastRecentlyUsed implements Comparator<CacheObjectDescriptor>{ 
	@Override 
	public int compare(CacheObjectDescriptor e1, CacheObjectDescriptor e2) { 
		if (e1.equals(e2)) return 0;
		if (e1.lastAccess < e2.lastAccess) {
			return 1;
		}else if (e1.lastAccess > e2.lastAccess){
			return -1;
		}else return new Integer(e1.numAccesses).compareTo(new Integer(e2.numAccesses));
	} 
}
class LeastFrequentlyUsed implements Comparator<CacheObjectDescriptor>{ 
	@Override 
	public int compare(CacheObjectDescriptor e1, CacheObjectDescriptor e2) { 
		if (e1.equals(e2)) return 0; //e1.objectId.compareTo(e2.objectId);
		//return new Integer(e1.getHits).compareTo(new Integer(e2.getHits));
		if (e1.getHits < e2.getHits) {
			return 1;
		}else if (e1.getHits > e2.getHits){
			return -1;
		}else return new Long(e1.lastAccess).compareTo(new Long(e2.lastAccess));
	} 
}	
