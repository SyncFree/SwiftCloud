#!/usr/bin/python
# Generate a command file for adding bogus users to a file
import sys, os, base64, random, time

activities = ["Running Experiments", "Drinking Coffee" , "Sleeping"]
allUsers = []

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
def doMixed(friends_per_user, commands_per_user_biased, commands_per_user, total_commands, number_of_sites, active_users):
  mix = []
  
  print "Number of sites: " + str(number_of_sites)
  for site in range(0,number_of_sites):
    print "generating output for site " + str(site)
    if len(sys.argv) == 9:
          outFile = sys.argv[8]+"-"+str(site)
          fOut = open(outFile, 'w')
    else:
          fOut = sys.stdout

    activeSessionsPerSite = active_users/number_of_sites
    partitionSize = len(allUsers)/number_of_sites
    print "Size of user partition: "+ str(partitionSize)
    for sessions in range(0,activeSessionsPerSite): 
        userName = allUsers[random.randint(0,partitionSize-1) + partitionSize * site]
    
    #generate the friends for that user
        friends=[]
        for i in range(0,friends_per_user):
           friends.append(allUsers[random.randint(0,len(allUsers)-1)])      
    
        for k, v in commands.items():
           for i in range(0,v[1]):
               mix.append(v[0])
    
            
        #login
        fOut.write('login;{0};passwd;\n'.format(userName))
        #operations on friends
        for j in range(0, total_commands):
            for i in range(0, commands_per_user_biased):
                fOut.write(mix[random.randint(0, len(mix)-1)](userName,friends))
        #operations on all users
            for i in range(0, commands_per_user):
                fOut.write(doReadLine(userName,allUsers))
        #logout
        fOut.write('logout;{0};\n'.format(userName))
    fOut.close()

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
  if (len(sys.argv) != 8 and len(sys.argv) != 9):
    print "Usage ", sys.argv[0],"<users_file> <number_of_friends> <ops_per_session_biased> <ops_per_session_random> <totoal_ops> <num_sites> <active_users> [<out_file>]"
    sys.exit()
  
  fName = sys.argv[1]
  friends_per_user = int(sys.argv[2])
  commands_per_user_biased = int(sys.argv[3])
  commands_per_user = int(sys.argv[4])
  total_commands = int(sys.argv[5])
  number_of_sites = int(sys.argv[6])
  active_users = int(sys.argv[7])


  print "sites: " + str(number_of_sites)

  random.seed(time.time())
  readUsers(fName)
  doMixed(friends_per_user, commands_per_user_biased, commands_per_user, total_commands, number_of_sites, active_users)

if __name__ == "__main__":
  main()

