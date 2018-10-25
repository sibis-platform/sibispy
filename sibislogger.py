##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

import sys
import json
# import logging
import collections
import time 
import os
import post_issues_to_github as pig
# set logger of python packages to warning so that we avoid info messages being printed out 
#logging.getLogger("urllib3").setLevel(logging.WARNING)
#logging.getLogger("requests").setLevel(logging.WARNING)

class sibisJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return json.JSONEncoder.default(self, obj)
        except TypeError as e:
            if hasattr(obj, '__call__') and hasattr(obj, '__name__'):
                return obj.__name__
            raise e
            


class sibisLogging():
    """
    SIBIS Logging Module
    """
    def __init__(self,config_file=None):
        # self.logging = logging

        # all call that are info or above will be printed out 
        # self.logging.basicConfig(level=logging.INFO, format='%(message)s')

        self.log = collections.OrderedDict()

        # Configure file to be used when positing things to github
        self.postGithubConfigFile = config_file
        self.postGithubRepo = None
        self.postGithubTitle = None
        self.postGithubLabel = None
        self.startTime1=None
        self.startTime2=None
        self.fileTime=None
        self.verbose = False

    def create_log(self,uid, message, **kwargs):
        # Turn message into a ordered dictionary
        self.log.update(experiment_site_id=uid,
                        error=message)
        self.log.update(kwargs)
        jlog = json.dumps(self.log, cls=sibisJSONEncoder)
        self.log.clear()
        return jlog
        
    def info(self, uid, message, **kwargs):
        """
        Replaces logging.info
        if postGithubRepo is defined then posts it to github instead of logger
        """
        jlog = self.create_log(uid, message, **kwargs)

        if self.postGithubRepo :
            if self.verbose:
                print "Posting", uid, str(message)

            return pig.create_issues_from_list(self.postGithubRepo, self.postGithubTitle, self.postGithubLabel, [jlog],self.verbose)

        # Post output to logger 
        # return self.logging.info(log)
        print jlog 
        return []

    def post_to_github(self,general_title,git_label):
        if self.verbose:
            print "================================"
            print "== Setting up posting to GitHub "

        # Create Connection
        if not self.postGithubRepo :
            self.postGithubRepo = pig.connect_to_github(config_file=self.postGithubConfigFile,verbose=self.verbose) 
        # Make sure it is a valid title 
        if not self.postGithubRepo: 
            return False

        self.postGithubLabel=pig.get_github_label(self.postGithubRepo, git_label, self.verbose)
        if not self.postGithubLabel :
            print "Warning: Github label does not exist so not creating issues on github!"
            self.postGithubRepo = None
            return False

        self.postGithubTitle=general_title + " (" + git_label + ")"
        if self.verbose:
            print "== Posting to GitHub is ready "
            print "================================"

        return True

    def initiateTimer(self,timerFile):
        timerDir = os.path.dirname(timerFile)
        if not os.path.exists(timerDir): 
            self.info("sibislogger.py","Error: Directory  " + timerDir + " does not exist - timer disabled") 
        else : 
            self.fileTime = timerFile

    def startTimer1(self):
        if self.fileTime  :
            self.startTime1 = time.time()

    def startTimer2(self):
        if self.fileTime  :
            self.startTime2 = time.time()

    def _stopTimerGeneral(self,timerID,label=None,info=None):
        if not self.fileTime : 
            return 
            
        if timerID == 1: 
            startTimer= self.startTime1
        else :
            startTimer= self.startTime2 
        
        if not startTimer: 
            return

        endTimer=time.time()
        time_date_format = '%Y-%m-%d %H:%M:%S'
        time_diff = int(1000*(endTimer - startTimer))
        time_diff_sec = int(time_diff / 1000)
        timeLine= ','.join([time.strftime(time_date_format,time.localtime(startTimer)),time.strftime(time_date_format,time.localtime(endTimer)),str(time_diff_sec/60) + ":" + str(time_diff_sec%60).zfill(2)  + ":" + str(time_diff%1000).zfill(3)])
        timeLine += ','
        if label : 
            timeLine += str(label)

        timeLine += ','
        if info : 
            timeLine += '"' + str(info) + '"'

        try :
            if os.path.isfile(self.fileTime) :
                fd = open(self.fileTime,'a')
            else :
                fd = open(self.fileTime,'w')
                fd.write("start-time,end-time,difference-min:sec:msec,label,extra-info\n")

            fd.write(timeLine +'\n')
            fd.close()
            

        except Exception as err_msg: 
            self.info('sibislogger','Error: Failed to write to time stamp to file',
                     fileTime=self.fileTime,
                     err_msg=str(err_msg)) 

    def takeTimer1(self,label=None,info=None):
        self._stopTimerGeneral(1,label,info)


    def takeTimer2(self,label=None,info=None):
        self._stopTimerGeneral(2,label,info)


#
# Specifically to raise an error that then can be easily posted to slgo.info ! 
#
class sibisExecutionError(Exception):
    def __init__(self, uid, msg, **kwargs):
        self.uid = uid 
        self.msg = msg
        self.info =kwargs

    def add(self, **kwargs):
        self.info.update(kwargs)

    def slog_post(self):
        if self.info :
            log.info(self.uid, self.msg, **self.info)
        else : 
            log.info(self.uid, self.msg)

    def __str__(self):
        if self.info :
            return str(log.create_log(self.uid, self.msg, **self.info))
        else : 
            return str(log.create_log(self.uid, self.msg))


def init_log(verbose=False,post_to_github=False,github_issue_title="",github_issue_label="",timerDir=None):
    global log
    log = sibisLogging()
    log.verbose = verbose
    if post_to_github: 
        log.post_to_github(github_issue_title, github_issue_label)
    
    if timerDir : 
        log.initiateTimer(os.path.join(timerDir,github_issue_label + "-time_log.csv"))

# if this fails bc it cannot find log, please make sure init_log is called first 
def info(uid, message, **kwargs):
    return log.info(uid,message,**kwargs)

def startTimer1():
    log.startTimer1()

def startTimer2():
    log.startTimer2()

def takeTimer1(label=None,info=None):
    log.takeTimer1(label,info)

def takeTimer2(label=None,info=None):
    log.takeTimer2(label,info)

