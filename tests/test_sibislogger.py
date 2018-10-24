#!/usr/bin/env python

##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

# if test script is run with argument then it will run script with the sibis config file defined by that argument 
# for example test_sibislogger.py ~/.sibis-general-config.yml 
# otherwise will run with data/.sibis-general-config.yml


from __future__ import print_function
from sibispy import sibislogger as slog
from sibispy import post_issues_to_github as pig
import os 
import sys
import time 

if sys.argv.__len__() > 1 : 
    config_file = sys.argv[1]
else :
    config_file= os.path.join(os.path.dirname(sys.argv[0]), 'data', '.sibis-general-config.yml')


# does not require config file as we do not parse to github 
slog.init_log(False, False,'test_sibislogger', 'test_sibislogger',None)

try : 
    raise slog.sibisExecutionError('label','title', test1 = '2', test2 = '4')
except slog.sibisExecutionError as err:
    print(str(err))
    err.add(test3 = '3')
    err.slog_post()

# test timer and posting to github 
log = slog.sibisLogging(config_file=config_file)

if os.path.isfile('/tmp/test_sibislogger-time_log.csv') : 
   os.remove('/tmp/test_sibislogger-time_log.csv') 

log.takeTimer1()
log.initiateTimer('/asaffsat/test_sibislogger-time_log.csv')
if log.fileTime :
   sys.exit(1)

log.initiateTimer('/tmp/test_sibislogger-time_log.csv') 
if not log.fileTime :
   sys.exit(1)

log.startTimer1() 
time.sleep(1)
log.takeTimer1('should be 0:01:001')
if not os.path.isfile('/tmp/test_sibislogger-time_log.csv') :
   sys.exit(1)

log.verbose = False

# just normal 

#print "============== Test 1 ==================="
#print "Push issue to logger"
blub = log.info('sibislogger_test_1',"Unit Testing 1", msg="Please ignore message")

# find out how to close issues from api 
# post to github 
#print "============== Test 2 ==================="
#print "Post issue on GitHub"
log.startTimer2()
issue_label_text='testing'
log.post_to_github('Unit Test', issue_label_text)
if log.postGithubRepo : 
  log.takeTimer2("Test 2: Post Github: Initialize")
  issue_label=pig.get_github_label(log.postGithubRepo,issue_label_text)
  
  iID="sibislogger_test_2"
  iTitle= "Testing 2"

  # print "Issue exists:",
  log.startTimer2()
  issue=pig.get_issue(log.postGithubRepo, iID + ", " +  iTitle,issue_label)
  log.takeTimer2("Test 2: Post Github: Get Issue")
  if issue:
    # print "yes, closed"
    log.startTimer2()
    issue.edit(state='close')
    log.takeTimer2("Test 2: Post Github: Change State")
  else : 
    print("No") 

  log.info(iID,iTitle, msg="Please ignore message")
  #print "============== Test 3 ==================="
  #print "Issue should already exist!"
  log.info('sibislogger_test_2',"Testing 2", msg="Please ignore message")

  # Close issue 
  issue=pig.get_issue(log.postGithubRepo, iID + ", " +  iTitle,issue_label)
  # Error: Could not find issue!"
  assert(issue)
  issue.edit(state='close')

else : 
   print("ERROR: Failed test posting issue to gtihub") 

log.takeTimer1()
print("Output of time log written to "  + log.fileTime) 

# log.info('Unit Test (testing)', 'sibislogger_test_2 (TESTING)',"Testing 2", msg="Please ignore message")

def some_function():
  pass

try:
  slog.info('sibislogger_test_3', "Logging kwargs ",
                        funkey = some_function,
                        strkey = "some string",
                        intkey = int(20),
                        floatkey = float(12.12345678),
                        listintkey = [ int(0), int(1), int(2), int(3) ],
                        liststrkey = [ "a", "b", "c" ],
                        listfloatkey = [ float(0.1), float(1.0), float(2.12345678) ],
                        objkey = { 
                          "strkey": "strval",
                          "intkey": int(9),
                          "floatkey":  float(9.87654321)
                        })
except Exception as e:
  print("ERROR: failed to log kwargs", str(e))