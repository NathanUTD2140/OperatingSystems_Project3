import sys
import os
import struct
import csv


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
