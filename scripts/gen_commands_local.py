#!/usr/bin/python
# Generate a command file for adding bogus users to a file
import sys, os, base64, random, time

activities = ["Running Experiments", "Drinking Coffee" , "Sleeping"]
allUsers = []
friends_of = []

commands_per_user_biased=5
commands_per_user=2
friends_per_user=1
number_of_sites = 1
sessions_per_site = 1

def doStatusLine(user,friends):
  return 'status;"{0}";\n'.format(activities[random.randint(0, len(activities)-1)])

def doPostLine(user,friends):
  recipient = friends[random.randint(0, len(friends) - 1)]
  return 'post;{0};"What up, dawg";\n'.format(recipient)

def doReadLine(user,friends):
  peer = friends[random.randint(0, len(friends) - 1)]
  return 'read;{0};\n'.format(peer)
  
def doFriendLine(user,friends):
  peer = friends[random.randint(0, len(friends) - 1)]
  return 'friend;{0};\n'.format(peer)

def doSeeFriendLine(user,friends):
  peer = friends[random.randint(0, len(friends) - 1)]
  return 'see_friends;{0};\n'.format(peer)

def doLogin(user):
  return 'login;{0};passwd;\n'.format(user)

def doLogout(user):
  return 'logout;{0};\n'.format(user)
  

def doMixedLine(user):
  return 0

# A mixed load
def doMixed():
  mix = []
  
  for sites in range(0,number_of_sites):
      #for j in range(0,sessions_per_site)  
   userName = allUsers[random.randint(0,len(allUsers) - 1)]
   for sessions in range(0,sessions_per_site): 
    
    #generate the friends for that user
    for i in range(0,friends_per_user):
       friends_of.append(allUsers[random.randint(0,len(allUsers)-1)])      
    
    for k, v in commands.items():
      for i in range(0,v[1]):
        mix.append(v[0])
    
    if len(sys.argv) == 6:
        outFile = sys.argv[5]+"-"+str(sites)
        fOut = open(outFile, 'w')
    else:
        fOut = sys.stdout
            
    #login
    fOut.write('login;{0};passwd;\n'.format(userName))
    #operations on friends
    for i in range(0, commands_per_user_biased):
      fOut.write(mix[random.randint(0, len(mix)-1)](userName,friends_of))
    #operations on all users
    for i in range(0, commands_per_user):
      fOut.write(doReadLine(userName,allUsers))
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
  if (len(sys.argv) != 5 and len(sys.argv) != 6):
    print "Usage ", sys.argv[0],"<users_file> <number_of_friends> <ops_per_session_biased> <ops_per_session_random>  [<out_file>]"
    sys.exit()
  
  fName = sys.argv[1]
  number_of_friends=int(sys.argv[2])
  ops_per_session_biased=int(sys.argv[3])
  ops_per_session_random=int(sys.argv[4])
  
  random.seed(time.time())
  readUsers(fName)
  doMixed()

if __name__ == "__main__":
  main()

