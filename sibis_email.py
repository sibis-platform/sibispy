from __future__ import print_function
##
##  See COPYING file distributed along with the ncanda-data-integration package
##  for the copyright and license terms
##

# Mail-related stuff
from builtins import str
from builtins import object
import smtplib
import time 
import json 
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from sibispy import sibislogger as slog

class sibis_email(object):
    """ Class handling email communication with XNAT users and admin."""

    # Initialize class.
    def __init__(self, smtp_server, admin_email, sibis_admin_email = None):
        self._sibis_admin_email = sibis_admin_email
        self._admin_email = admin_email
        self._admin_messages = []
        self._messages_by_user = dict()
        self._smtp_server = smtp_server
        
    # Add to the message building up for a specific user
    def add_user_message( self, uid, txt, uFirstName=None, uLastName=None, uEmail=None):
        if uid not in self._messages_by_user:
            self._messages_by_user[uid] = {'uFirst': uFirstName, 'uLast': uLastName, 'uEmail' : uEmail,  'msgList' : [txt] }
        else:
            self._messages_by_user[uid]['msgList'].append( txt )

    # Add to the message building up for the admin
    def add_admin_message( self, msg ):
        self._admin_messages.append( msg )

    # Send pre-formatted mail message 
    def send( self, subject, from_email, to_email, html, sendToAdminFlag=True ):
        if not self._smtp_server : 
            slog.info("sibis_email.send","ERROR: smtp server not defined - email will not be sent!")
            return False

        if not to_email :
            slog.info("sibis_email.send","ERROR: no email address for recipient defined - email will not be sent!")
            return False

        # Create message container - the correct MIME type is multipart/alternative.
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = from_email
        msg['To'] = ', '.join( to_email )

        # Record the MIME types of both parts - text/plain and text/html.
        text = ''
        part1 = MIMEText(text, 'plain')
        part2 = MIMEText(html, 'html')
    
        # Attach parts into message container.
        # According to RFC 2046, the last part of a multipart message, in this case
        # the HTML message, is best and preferred.
        msg.attach(part1)
        msg.attach(part2)
    
        # Send the message via local SMTP server.
        try : 
            s = smtplib.SMTP( self._smtp_server )
        except Exception as err_msg:
            slog.info("sibis_email.send","ERROR: failed to connect to SMTP server at {} ".format(time.asctime()),
                    err_msg = str(err_msg),
                    smtp_server = self._smtp_server) 
            return False
 
        try : 
            # sendmail function takes 3 arguments: sender's address, recipient's address
            # and message to send - here it is sent as one string.
            s.sendmail( from_email, to_email, msg.as_string() )
        
            # Send email also to sibis admin if defined
            if sendToAdminFlag and self._sibis_admin_email and to_email != self._sibis_admin_email : 
                s.sendmail( from_email, self._sibis_admin_email, msg.as_string() )

        except Exception as err_msg:
            slog.info("sibis_email.send","ERROR: failed to send email at {} ".format(time.asctime()),
                      err_msg = str(err_msg),
                      email_from = from_email, 
                      email_to = to_email,
                      sibis_admin_email = self._sibis_admin_email, 
                      email_msg = msg.as_string(),
                      smtp_server = self._smtp_server)
            s.quit()
            return False


        s.quit()
        return True

    # Send mail to one user
    def mail_user( self, uEmail, uFirst, uLast, title, intro_text, prolog,  msglist ):
        problem_list = [ '<ol>' ]
        for m in msglist:
            problem_list.append( '<li>%s</li>' % m )
        problem_list.append( '</ol>' )
            
        # Create the body of the message (a plain-text and an HTML version).
        html = '<html>\n <head></head>\n <body>\n <p>Dear %s %s:<br><br>\n' %(uFirst,uLast) + intro_text + '\n %s\n' % ('\n'.join( problem_list ) ) + prolog + '\n</p>\n</body>\n</html>' 
        
        self.send( title, self._admin_email, [ uEmail ], html )

    def mail_admin( self, title, intro_text):
        problem_list = []
        if len( self._messages_by_user ) > 0:
            problem_list.append( '<ul>' )
            for (uid,info_msglist) in self._messages_by_user.items():
                problem_list.append( '<li>User: %s %s (%s)</li>' % (info_msglist['uFirst'],info_msglist['uLast'],info_msglist['uEmail']) )
                problem_list.append( '<ol>' )
                for m in info_msglist['msgList']:
                    problem_list.append( '<li>%s</li>' % m )
                problem_list.append( '</ol>' )
            problem_list.append( '</ul>' )

        if len( self._admin_messages ) > 0:
            problem_list.append( '<ol>' )
            for m in self._admin_messages:
                problem_list.append( '<li>%s</li>' % m )
            problem_list.append( '</ol>' )            

        text = ''

        # Create the body of the message (a plain-text and an HTML version).
        html = '<html>\n\
<head></head>\n\
<body>\n' + intro_text + '\n %s\n\
</p>\n\
</body>\n\
</html>' % ('\n'.join( problem_list ))
    
        self.send(title, self._admin_email, [ self._admin_email ], html )

    def send_all( self, title, uIntro_txt, uProlog, aIntro_txt ):
        # Run through list of messages by user
        if len( self._messages_by_user ):
            for (uid,uInfo_msg) in self._messages_by_user.items():
               self.mail_user(uInfo_msg['uEmail'],uInfo_msg['uFirst'],uInfo_msg['uLast'], title, uIntro_txt, uProlog,  uInfo_msg['msgList'])

        if len( self._messages_by_user ) or len( self._admin_messages ):
            self.mail_admin(title, aIntro_txt)

    def dump_all( self ):
        print("USER MESSAGES:")
        print(self._messages_by_user)
        print("ADMIN_MESSAGES:")
        print(self._admin_messages)

class xnat_email(sibis_email):
    def __init__(self, session): 
        self._interface = session.api['xnat']
        if self._interface :
            try: 
                # XNAT 1.7
                server_config = self._interface.client.get('/xapi/siteConfig').json()
            except Exception as ex:
                # XNAT 1.6
                server_config = self._interface._get_json('/data/services/settings')
            self._site_url = server_config[u'siteUrl']
            self._site_name = server_config[u'siteId']
            sibis_email.__init__(self,server_config[u'smtp_host'],server_config[u'adminEmail'],session.get_email())

        else: 
            slog.info('xnat_email.__init__',"ERROR: xnat api is not defined")
            self._site_url = None 
            self._site_name = None  
            sibis_email.__init__(self,None,None,session.get_email())

        self._project_name = session.get_project_name()
        # Determine server config to get admin email and public URL


    def add_user_message( self, uname, msg ):
        if uname not in self._messages_by_user:
            try: 
                user = self._interface.client.users[uname]
                uEmail = user.email
                user_firstname = user.first_name
                user_lastname = user.last_name
            except:
                slog.info('xnat_email.add_user_message',"ERROR: failed to get detail information for user " + str(uname) + " at {}".format(time.asctime()),
                          msg = str(msg))
                return False

            sibis_email.add_user_message(self,uname,msg,user_firstname,user_lastname,uEmail)
        else:
            sibis_email.add_user_message(self,uname,msg)

        return True


    def mail_user( self, uEmail, uFirst, uLast, msglist ):
        intro='We have detected the following problem(s) with data you uploaded to the <a href="%s">%s XNAT image repository</a>:' % (self._site_url, self._site_name)
        prolog='Please address these issues as soon as possible (direct links to the respective data items are provided above for your convenience). If you have further questions, feel free to contact the  <a href="mailto:%s">%s support</a>' % (self._admin_email, self._project_name )
        
        title="%s XNAT: problems with your uploaded data" % ( self._project_name )
        sibis_email.mail_user(self,uEmail, uFirst, uLast, title, intro, prolog,  msglist)


    def mail_admin(self):
        title = "$s: %s XNAT problem update" % (self._project_name, self._site_name)
        intro_text = 'We have detected the following problem(s) with data on <a href="%s">%s XNAT image repository</a>:' % (self._site_url, self._project_name)
        sibis_email.mail_admin(self,title, intro_text)


    def send_all( self ):
        # Run through list of messages by user
        if len( self._messages_by_user ):
            for (uname,info_msglist) in self._messages_by_user.items():
                self.mail_user(info_msglist['uEmail'], info_msglist['uFirst'], info_msglist['uLast'], info_msglist['msgList'])

        if len( self._messages_by_user ) or len( self._admin_messages ):
            self.mail_admin

