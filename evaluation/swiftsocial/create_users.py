#!/usr/bin/python
# Generate a command file for adding bogus users to a file
# TODO: CLARIFY LICENSING: THIS SCRIPT COMES FROM WALTER PAPER PUBLISHED IN OSDI 2011

import sys, os, base64, hashlib

def main():
  numUsers=0
  
  if (len(sys.argv) != 2 and len(sys.argv) != 3):
    print "Usage ", sys.argv[0],"<user_count> [<output_file>]"
    sys.exit()
  
  numUsers = int(sys.argv[1])

  if len(sys.argv) == 3:
    outFile = sys.argv[2]
    f = open(outFile, 'w')
  else:
    f = sys.stdout
    
  print "Start with generation..."

  for i in range(0, numUsers):
      name = base64.b64encode(os.urandom(6))
      m = hashlib.md5()
      m.update(name)
      hex_oid = int(m.hexdigest()[0:16], 16)
    # verify that the name is in the range
      f.write('usr_add;{0};passwd;"{0} {0}son";01/01/01;"";\n'.format(name))
  
  print "Finished creating", numUsers, "users."
  if len(sys.argv) == 3:
    print "Result in:", sys.argv[2]
    
if __name__ == "__main__":
  main()
