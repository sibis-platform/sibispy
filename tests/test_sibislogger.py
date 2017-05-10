#!/usr/bin/env python

##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

from sibisAlpha import sibislogger
from sibisAlpha import post_issues_to_github as pig

log = sibislogger.sibisLogging()
log.verbose = True
# just normal 
print "============== Test 1 ==================="
print "Push issue to logger"
blub = log.info('sibislogger_test_1',"Unit Testing 1", msg="Please ignore message")
# find out how to close issues from api 
# post to github 
print "============== Test 2 ==================="
print "Post issue on GitHub"
log.post_to_github('Unit Test', 'testing')

iID="sibislogger_test_2"
iTitle= "Testing 2"

print "Issue exists:",
issue=pig.get_issue(log.postGithubRepo, iID + ", " +  iTitle, False)
if issue:
    print "yes, closed"
    issue.edit(state='close')
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
 

# log.info('Unit Test (testing)', 'sibislogger_test_2 (TESTING)',"Testing 2", msg="Please ignore message")
