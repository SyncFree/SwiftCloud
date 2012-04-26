#!/usr/bin/python
# Generate a command file for adding bogus users to a file
import sys, os, base64, hashlib

def main():
  numUsers=0
  
  if (len(sys.argv) != 3 and len(sys.argv) != 4):
    print "Usage ", sys.argv[0],"<max_user_oid> <user_count> [<output_file>]"
    sys.exit()
  
  max_oid = int(sys.argv[1], 16)
  numUsers = int(sys.argv[2])

  if len(sys.argv) == 4:
    outFile = sys.argv[3]
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

if __name__ == "__main__":
  main()
