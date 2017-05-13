#!/usr/bin/env python

##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

from sibispy import sibislogger
from sibispy import post_issues_to_github as pig
import os 
import sys
import time 

log = sibislogger.sibisLogging()

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
log.post_to_github('Unit Test', 'testing')
log.takeTimer2("Test 2: Post Github: Initialize")

iID="sibislogger_test_2"
iTitle= "Testing 2"

print "Issue exists:",
log.startTimer2()
issue=pig.get_issue(log.postGithubRepo, iID + ", " +  iTitle, False)
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
issue=pig.get_issue(log.postGithubRepo, iID + ", " +  iTitle, True)
if issue:
    issue.edit(state='close')
else : 
    print "Error: Could not find issue!"
 
log.takeTimer1()
print "Output of time log written to "  + log.fileTime 

# log.info('Unit Test (testing)', 'sibislogger_test_2 (TESTING)',"Testing 2", msg="Please ignore message")
