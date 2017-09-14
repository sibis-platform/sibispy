#!/usr/bin/env python

##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

# if test script is run with argument then it will run script with the sibis config file defined by that argument 
# for example test_sibislogger.py ~/.sibis-general-config.yml 
# otherwise will run with data/.sibis-general-config.yml


from sibispy import sibislogger
from sibispy import post_issues_to_github as pig
import os 
import sys
import time 

if sys.argv.__len__() > 1 : 
    config_file = sys.argv[1]
else :
    config_file= os.path.join(os.path.dirname(sys.argv[0]), 'data', '.sibis-general-config.yml')

log = sibislogger.sibisLogging(config_file=config_file)

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

log.verbose = True

# just normal 

print "============== Test 1 ==================="
print "Push issue to logger"
blub = log.info('sibislogger_test_1',"Unit Testing 1", msg="Please ignore message")

# find out how to close issues from api 
# post to github 
print "============== Test 2 ==================="
print "Post issue on GitHub"
log.startTimer2()
issue_label_text='testing'
log.post_to_github('Unit Test', issue_label_text)
if log.postGithubRepo : 
  log.takeTimer2("Test 2: Post Github: Initialize")
  issue_label=pig.get_github_label(log.postGithubRepo,issue_label_text)
  
  iID="sibislogger_test_2"
  iTitle= "Testing 2"

  print "Issue exists:",
  log.startTimer2()
  issue=pig.get_issue(log.postGithubRepo, iID + ", " +  iTitle,issue_label)
  log.takeTimer2("Test 2: Post Github: Get Issue")
  if issue:
    print "yes, closed"
    log.startTimer2()
    issue.edit(state='close')
    log.takeTimer2("Test 2: Post Github: Change State")
  else : 
    print "No" 

  log.info(iID,iTitle, msg="Please ignore message")
  print "============== Test 3 ==================="
  print "Issue should already exist!"
  log.info('sibislogger_test_2',"Testing 2", msg="Please ignore message")

  # Close issue 
  issue=pig.get_issue(log.postGithubRepo, iID + ", " +  iTitle,issue_label)
  # Error: Could not find issue!"
  assert(issue)
  issue.edit(state='close')

else : 
   print "ERROR: Failed test posting issue to gtihub" 

log.takeTimer1()
print "Output of time log written to "  + log.fileTime 

# log.info('Unit Test (testing)', 'sibislogger_test_2 (TESTING)',"Testing 2", msg="Please ignore message")
