#!/usr/bin/python
# Generate a command file for adding bogus users to a file
import sys, os, base64, random, time

activities = ["Running Experiments", "Drinking Coffee" , "Sleeping"]
allUsers = []
commands_per_user=10

def doStatusLine(user):
  return 'status;"{0}";\n'.format(activities[random.randint(0, len(activities)-1)])

def doPostLine(user):
  recipient = allUsers[random.randint(0, len(allUsers) - 1)]
  return 'post;{0};"What up, dawg";\n'.format(recipient)

def doReadLine(user):
  peer = allUsers[random.randint(0, len(allUsers) - 1)]
  return 'read;{0};\n'.format(peer)
  
def doFriendLine(user):
  peer = allUsers[random.randint(0, len(allUsers) - 1)]
  return 'friend;{0};\n'.format(peer)

def doSeeFriendLine(user):
  peer = allUsers[random.randint(0, len(allUsers) - 1)]
  return 'see_friends;{0};\n'.format(peer)

def doLogin(user):
  return 'login;{0};passwd;\n'.format(user)

def doLogout(user):
  return 'logout;{0};\n'.format(user)
  

def doMixedLine(user):
  return 0

# A mixed load
def doMixed(commandCount, fOut):
  mix = []
  for k, v in commands.items():
    for i in range(0,v[1]):
      mix.append(v[0])
  for i in range(0, commandCount/commands_per_user):
    userName = allUsers[i % len(allUsers) - 1]
    #login
    fOut.write('login;{0};passwd;\n'.format(userName))
    for i in range(0, commands_per_user):
      fOut.write(mix[random.randint(0, len(mix)-1)](userName))
    #logout
    fOut.write('logout;{0};\n'.format(userName))

# How to distribute the operations
commands = {
            'status' : (doStatusLine , 5),
            'post' : (doPostLine, 5),
            'read' : (doReadLine, 80),
            'friend' : (doFriendLine , 2),
            'see_friends' : (doSeeFriendLine , 8),
            'mixed' : (doMixedLine , 0),
            }

def readUsers(usersFile):
  fIn = open(usersFile, 'r')
  for line in fIn:
    allUsers.append(line.split(';')[1])

def main():
  numUsers=0
  if (len(sys.argv) != 4 and len(sys.argv) != 5):
    print "Usage ", sys.argv[0],"<command_type> <user_file> <count> [<out_file>]"
    sys.exit()
  
  commandType = sys.argv[1]
  fName = sys.argv[2]
  
  if len(sys.argv) == 5:
    outFile = sys.argv[4]
    fOut = open(outFile, 'w')
  else:
    fOut = sys.stdout
  
  commandCount = int(sys.argv[3])
  
  foundOne = False
  doCommand = doStatusLine
  
  for k, v in commands.items():
    if sys.argv[1] == k:
      foundOne = True
      doCommand = v[0]
  
  if not foundOne:
    print 'supported commands'
    for k, v in commands.items():
      print k
    sys.exit(-1)
  
  random.seed(time.time())
  readUsers(fName)

  if sys.argv[1] == 'mixed':
    doMixed(commandCount, fOut)
  else:
    for i in range(0, commandCount/commands_per_user):
        # Choose a user to run the next 10 commands under
        #userName = allUsers[random.randint(0, len(allUsers) - 1)]
        userName = allUsers[i % len(allUsers) - 1]
        #login
        fOut.write('login;{0};passwd;\n'.format(userName))
        for i in range(0, commands_per_user):
          fOut.write(doCommand(userName))
        #logout
        fOut.write('logout;{0};\n'.format(userName))

if __name__ == "__main__":
  main()

