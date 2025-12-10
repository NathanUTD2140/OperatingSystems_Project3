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
        self.numOfKeys = 0 # number of keys currently in there
        self.keys = [0] * max_keys # the 19 keys associated with it
        self.values = [0] * max_keys #the 19 values associated with it
        self.children = [0] * children_num #list of potential children, currently 0

    def is_leaf(self):
        return all(cid == 0 for cid in self.children) #If all the children are 0, then it is a leaf node

    def to_bytes(self):
        bytebuffer = bytearray() # create a mutable byte buffer to assemble the block
        bytebuffer += int_to_bytes(self.block_id) #appending the 8 bytes for the header (big endian)
        bytebuffer += int_to_bytes(self.parent) #appends the 8 bytes for the parent of this node (will be 0 if root)
        bytebuffer += int_to_bytes(self.numOfKeys) #appends the 8 bytes for the number of keys that will be used
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
        node.numOfKeys = read8bytes() #reads the numbers of keys from the next 8 bytes that are in n
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
