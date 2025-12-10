import sys
import os
import struct
import csv
from collections import OrderedDict, deque

# constants listed in the document
byte_blocks = 512
magic_number = b'4348PRJ3'  # 8 bytes ASCII
# minimal degree
min_degree = 10
max_keys = 2 * min_degree - 1   # 19
children_num = 2 * min_degree   # 20

# Header layout: offset 0: 8 bytes magic, offset 8: 8 bytes root block id (0 if empty), offset 16: 8 bytes next block id
# unused for the rest of 512 bytes

# Node layout: 8 bytes of their block id, 8 bytes parent's block id, 8 bytes number of pairs in this block, 152 bytes 19 keys corresponding to values in the next chuck of bytes 152 bytes of 19 values with keys in previous blocks 
# 160 bytes if 20 child pointers to the next files
# rest unused 

def int_to_bytes(n):
    return int(n).to_bytes(8, byteorder='big', signed=True) # signed = true allows negative keys to be used, but it will be converted to unsigned 

def bytes_to_int(b):
    return int.from_bytes(b, byteorder='big', signed=True) #opposite method to convert it from bytes, signed still has to be true 

class Node:
    #read node layout for what will we do with this
    def __init__(self, block_id=0): #initializes it, assume it is 0 at first
        self.block_id = block_id # if a parameter is passed, we will make it
        self.parent = 0 #assume nothing at first
        self.n = 0 # number of keys currently in there
        self.keys = [0] * max_keys # the 19 keys associated with it
        self.values = [0] * max_keys #the 19 values associated with it
        self.children = [0] * children_num #list of potential children, currently 0

    def is_leaf(self):
        return all(cid == 0 for cid in self.children) #If all the children are 0, then it is a leaf node

    def to_bytes(self):
        bytebuffer = bytearray() # create a mutable byte buffer to assemble the block
        bytebuffer += int_to_bytes(self.block_id) #appending the 8 bytes for the header (big endian)
        bytebuffer += int_to_bytes(self.parent) #appends the 8 bytes for the parent of this node (will be 0 if root)
        bytebuffer += int_to_bytes(self.n) #appends the 8 bytes for the number of keys that will be used
        # keys
        for i in range(max_keys): #loop through all keys
            bytebuffer += int_to_bytes(self.keys[i]) #appends every 8 bytes for the keys associated in this part of the node
        # values
        for i in range(max_keys): #loop through all the keys
            bytebuffer += int_to_bytes(self.values[i]) # appends every 8 bytes and puts the values into it
        # children
        for i in range(children_num): #loop through the children number
            bytebuffer += int_to_bytes(self.children[i]) #puts in the children block ids for the pointers
        # pad to byte_blocks
        if len(bytebuffer) > byte_blocks: #make sure we don't accidentally go over the block size
            raise RuntimeError("Node serialization exceeded block size")
        bytebuffer += bytes(byte_blocks - len(bytebuffer))
        return bytes(bytebuffer)

    @staticmethod
    def from_bytes(data):
        if len(data) != byte_blocks:  # Makes sure we get the right block length
            raise ValueError("Invalid block size for node")
        offset = 0 #read the offset, starts at 0 and increments
        def read8bytes():
            nonlocal offset
            val = bytes_to_int(data[offset:offset+8]) # read 8 bytes from current offset and converts it to and integer, reads from 0-8
            offset += 8 #increments the offset
            return val #return the value here
        node = Node() #creates a new node
        node.block_id = read8bytes() #reads the first 8 bytes, will get the block_id
        node.parent = read8bytes() # parent block id from the next 8 bytes
        node.n = read8bytes() #reads the numbers of keys from the next 8 bytes that are in n
        # keys
        for i in range(max_keys): #in each key slot
            node.keys[i] = read8bytes() #read the 8bytes and put it in the list of keys
        # values
        for i in range(max_keys): # for each max key value
            node.values[i] = read8bytes() # read the 8bytes and put in the list of values
        # children
        for i in range(children_num): #for each child id
            node.children[i] = read8bytes() #read the 8 bytes and store it into the list of children (0 in all slots means its a leaf node)
        return node #return the list now filled with the node values

class BTreeFile:
    def __init__(self, path):
        self.path = path # we need to keep the path available to write
        # openInrw() will make it writable to, not just readable.
        self.fp = None  #There might be none if there is nothing to handle too
        self.root = 0 # block id of the B-tree root node. if it is 0, then it's empty.
        self.next_block = 1  #block id of the next unused block; the first allocated data block is 1 after the header
        self.node_cache = OrderedDict() # OrderedDict mapping block_id is the node. Keeps the most recent at the end

    # simple file operations
    def openInrw(self):
        if self.fp is None:
            self.fp = open(self.path, 'r+b') #open the file path in read and write mode

    def openInro(self): #read only opening
        if self.fp is None:
            self.fp = open(self.path, 'rb')#simply opens it only in read only mode

    def close(self): #this will close the file if it is write mode for the most part, but I can use it for read only 
        if self.fp:
            self.fp.close()
            self.fp = None #resets it after closing
            
    def _block_offset(self, block_id)
        return block_id * byte_blocks

    def readsHeader(self, must_exist=True):
        if not os.path.exists(self.path):
            if must_exist:
                raise FileNotFoundError(f"{self.path} does not exist") #cannot find the file current
            else:
                return #couldn't find it, even if it does exist

        with open(self.path, 'rb') as f: #opens it in a read mode
            data = f.read(byte_blocks) #rreads the data currently on the file
            if len(data) < 24: #this is the wrong kind of index file if the length is less then 24 bytes
                raise ValueError("Index file header too small or invalid")# header does not have our correct information
            magic = data[0:8] #magic number is in the first 8 bytes
            if magic != magic_number: #if they don't match up, not the right kind of index file
                raise ValueError("Not a valid index file (wrong magic header)")
            self.root = bytes_to_int(data[8:16]) #gets the root from these bytes (next bytes after magic number)
            self.next_block = bytes_to_int(data[16:24]) #gets the next block id from the bytes after the root id

    def writesHeader(self):
        self.openInrw() #opens it in the read write mode this time
        # build header buffer
        buf = bytearray()
        buf += magic_number # The first 8 bits is the magic number
        buf += int_to_bytes(self.root) # Next 8 is the correct root id
        buf += int_to_bytes(self.next_block) # Final 8 is the next block
        # error handling
        if len(buf) > byte_blocks:
            raise RuntimeError("Header serialization exceeded block size")
        buf += bytes(byte_blocks - len(buf)) #adjust buffer to the remaining length
        self.fp.seek(self._block_offset(0)) # We need to write from block 0, since this is the header
        self.fp.write(buf) #writes it
        self.fp.flush() # flush so changes last

    def validate_header(self):
        self.readsHeader(must_exist=True) #makes sure the header is still correct and has the magic number

    def allocate_node(self):
        new_id = self.next_block #sets up the next node with the next block available
        self.next_block += 1 #increment for the next block
        node = Node(block_id=new_id) #makes a new node for what will happen next
        self.write_node(node) # write node out and put it into the cache
        self.writesHeader()# modify header so we can update next block
        return node

# Node cache and creation

    def _cache_put(self, node):
        bid = node.block_id #puts the node current id as the this one
        # move existing entry to the end (mark as most-recent)
        if bid in self.node_cache:
            del self.node_cache[bid] #puts them out
        self.node_cache[bid] = node
        # evict if there are more then 3 nodes
        while len(self.node_cache) > 3: #if it is greater
            old_bid, old_node = self.node_cache.popitem(last=False) #pop the item from the cache
            self._write_node_to_disk(old_node)# write evicted node back to disk to persist any changes

    def _write_node_to_disk(self, node):
        self.openInrw() #opens it in read/write mode
        data = node.to_bytes() #gets the data from node
        self.fp.seek(self._block_offset(node.block_id)) #apply the offset
        self.fp.write(data) #writes the data
        self.fp.flush() #flush it out

    def writesNode(self, node):
        self._cache_put(node)# put node in the cache, if it gets evicted, it will get out of the cache
        self._write_node_to_disk(node)# writes it to disk afterwards, helps function

    def readsNode(self, block_id):
        # cache fast-path
        if block_id in self.node_cache:
            node = self.node_cache.pop(block_id) #move it to the end of the queue aka cache
            self.node_cache[block_id] = node# re-insert as most-recent and return
            return node

        with open(self.path, 'rb') as f: #read only mode to read from the disk
            f.seek(self._block_offset(block_id))
            data = f.read(byte_blocks) #read happens
            if len(data) != byte_blocks:
                # Handles if the file being read from has something happen to it
                raise RuntimeError(f"Failed to read full block for block id {block_id}")
            node = Node.from_bytes(data)# gets the data from bytes

        # add node to cache (may evict other nodes)
        self._cache_put(node) #put the node in the cache
        return node # returns it
# B-tree operations
    def create(self):
        if os.path.exists(self.path): #the file already exists, error below
            print("Error: file already exists", file=sys.stderr) #tell user it exists
            sys.exit(1)

        with open(self.path, 'wb') as f: #open in write mode
            buf = bytearray()
            buf += magic_number #put in our magic number as the header
            buf += int_to_bytes(0)  # root id is 0 since there is a new file
            buf += int_to_bytes(1)  # next block id will be 1
            buf += bytes(byte_blocks - len(buf))  # pad to full block
            f.write(buf) #write this out
        print(f"Created index file {self.path}") #tell user it was created

    def openAndLoadHeader(self):
        if not os.path.exists(self.path):#the file already exists, error below
            print("Error: file does not exist", file=sys.stderr) #tell user it exists
            sys.exit(1)
        self.readsHeader(must_exist=True) #checks the read header and it needs to be correct

    def searchKey(self, key):
        if self.root == 0: # returns nothing because it is empty!
            return None, None
        return self._search_in_node(self.root, key) #calls helper function and returns it

    def _search_in_node(self, block_id, key):
        node = self.readsNode(block_id) #reads the node based on the block_id given
        # linear scan in node (since keys are stored in-order)
        i = 0
        while i < node.n and key > node.keys[i]: #increment i
            i += 1
        if i < node.n and key == node.keys[i]: #if the key matches the one passed, return the incrementation number and the node
            return node, i
        # not in this node; if leaf -> not found
        if node.is_leaf():
            return None, None #couldn't find it
        # otherwise go to the next child
        child_bid = node.children[i]
        if child_bid == 0:
            # bad tree or leaf node
            return None, None
        return self._search_in_node(child_bid, key) #try again in other node

    def insert(self, key, value):
        self.openAndLoadHeader() #validates header is good first
        if self.root == 0: #this is the root node
            # create root node
            new_root = self.allocate_node() #make space for the root
            new_root.parent = 0 #like in the document, 0 is the parent since it is the root
            new_root.n = 1 #simply put it as 1 cause nothing is in here right now
            new_root.keys[0] = key #parameter passed in
            new_root.values[0] = value #second argument from user
            self.root = new_root.block_id # blockid of the root
            # persist root node and header
            self.writesNode(new_root) #writes it
            self.writesHeader() #makes the header
            return #exit since we made the root node

        root_node = self.readsNode(self.root) 
        if root_node.n == max_keys: #roots full, make a new one
            # we need to split up
            s = self.allocate_node() #allocate the new node
            s.parent = 0 #initialization
            s.n = 0
            s.children[0] = root_node.block_id
            root_node.parent = s.block_id
            self.writesNode(root_node)  # update old root parent pointer on disk
            self.root = s.block_id #put it in as the new root
            self.writesNode(s) #write the node
            self.writesHeader()  # persist new root id
            self._split_child(s, 0) #splits the child
            self._insert_nonfull(s, key, value) #now put it into the nonfull one
        else:
            self._insert_nonfull(root_node, key, value) #otherwise just pass it to the new function

    def _insert_nonfull(self, node, key, value):
        i = node.n - 1 #sets i to the last key node index
        if node.is_leaf(): #checks if it is a leaf or not
            # shift keys/values rightwards to make room for new key
            pos = node.n #inserion index equals number of keys first
            while pos > 0 and key < node.keys[pos-1]: # if we're not at the start, key should come before the previous key
                node.keys[pos] = node.keys[pos-1] #moves the key once to the right
                node.values[pos] = node.values[pos-1] # moves the value to the right
                pos -= 1 #decrement
            node.keys[pos] = key #writes the key to the new clearly position
            node.values[pos] = value #writes the value
            node.n += 1 #increment the key count
            self.writesNode(node)
        else:
            # find our child node to go down
            while i >= 0 and key < node.keys[i]: #move the key left until it is greater than keys[i] or i is less then 0
                i -= 1 #decrement
            i += 1 #if child index is i + 1
            child_bid = node.children[i] #read the block id of the child
            if child_bid == 0:
                # child pointer missing means making a new child, insert there
                child = self.allocate_node()  #calls function
                child.parent = node.block_id # set the parent as the first point
                child.n = 1 #only one key after
                child.keys[0] = key # put the key into first slot
                child.values[0] = value #value corresponding to key
                node.children[i] = child.block_id # link the child id to the parent's array
                # persist child and parent updates
                self.writesNode(child)
                self.writesNode(node)
                return
            child = self.readsNode(child_bid) #read the child node
            if child.n == max_keys: #we're full, need to split it
                # split child so we have room to insert
                self._split_child(node, i) #splits it into two nodes now
                # after split, decide which of the two children to descend into
                if key > node.keys[i]: #if it is greater
                    i += 1 #increments the index
                child = self.readsNode(node.children[i])
            self._insert_nonfull(child, key, value) #recursively put more into the child node

    def _split_child(self, parent_node, i):
        y = self.readsNode(parent_node.children[i]) #read the node of the children
        z = self.allocate_node()
        z.parent = parent_node.block_id

        # copies the last min_degree-1 keys and values to z
        for j in range(min_degree, max_keys):  # j runs min_degree tp max_keys-1
            z.keys[j - min_degree] = y.keys[j]
            z.values[j - min_degree] = y.values[j]

        # copies last min_degree children to z also, since it's a different number need to do this
        for j in range(min_degree, children_num):
            z.children[j - min_degree] = y.children[j]

        # set counts
        z.n = min_degree - 1
        y.n = min_degree - 1

        # update parent pointers of moved children (if any)
        for j in range(min_degree):
            cid = z.children[j]
            if cid != 0:
                childnode = self.readsNode(cid) #read the childnode based off the child id
                childnode.parent = z.block_id #put z block as the paren
                self.writesNode(childnode) #write it

        # insert z as parent's child i+1, make room by shifting
        for j in range(parent_node.n, i, -1):
            parent_node.children[j + 1] = parent_node.children[j] #moves and shifts the child
        parent_node.children[i + 1] = z.block_id # put the final one as z's id

        # shift parent's keys/values to make space
        for j in range(parent_node.n - 1, i - 1, -1):
            parent_node.keys[j + 1] = parent_node.keys[j]
            parent_node.values[j + 1] = parent_node.values[j]

        # move median key from y up to parent
        parent_node.keys[i] = y.keys[min_degree - 1]
        parent_node.values[i] = y.values[min_degree - 1]
        parent_node.n += 1

        # clears the moved slots, makes sure it's correct
        for j in range(min_degree - 1, max_keys):
            y.keys[j] = 0
            y.values[j] = 0
        for j in range(min_degree, children_num):
            y.children[j] = 0

        # writes them to the disk
        self.writesNode(y) 
        self.writesNode(z)
        self.writesNode(parent_node)


def usage_and_exit():
    print("Usage:")
    print("  project3.py create <indexfile>")
    print("  project3.py insert <indexfile> <key> <value>")
    print("  project3.py search <indexfile> <key>")
    print("  project3.py load <indexfile> <csvfile>")
    print("  project3.py print <indexfile>")
    print("  project3.py extract <indexfile> <csvfile>")
    sys.exit(1)

def main():
    if len(sys.argv) < 2:
        usage_and_exit()
    cmd = sys.argv[1].lower()
    args = sys.argv[1:]
    if cmd == "create":
        cmd_create(args)
    elif cmd == "insert":
        cmd_insert(args)
    elif cmd == "search":
        cmd_search(args)
    elif cmd == "load":
        cmd_load(args)
    elif cmd == "print":
        cmd_print(args)
    elif cmd == "extract":
        cmd_extract(args)
    else:
        usage_and_exit()

if __name__ == "__main__":
    main()
