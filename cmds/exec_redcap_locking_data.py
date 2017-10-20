#!/usr/bin/env python
import sys
import argparse 
import sibispy

from sibispy import sibislogger as slog
from sibispy import redcap_locking_data 

def main(args=None):
    if not args:
        return 
    
    slog.startTimer1()
    session = sibispy.Session()
    if not session.configure() :
        if args.verbose:
            print "Error: session configure file was not found"

        sys.exit(1)

    engine = session.connect_server('redcap_mysql_db', True)
    if not engine :
        if args.verbose:
            print "Error: Could not connect to REDCap mysql db"

        sys.exit(1)

    if args.verbose:
        print "Connected to REDCap using: {0}".format(engine)

    red_lock = redcap_locking_data.redcap_locking_data()
    red_lock.configure(session)
    

    if args.unlock and args.lock:
        raise Exception("Only specify --lock or --unlock, not both!")

    for event_desc in args.event.split(',') :
        if args.verbose: 
            print "Visit: {0}".format(event_desc)

        if args.lock:
            if args.verbose:
                print "Attempting to lock form: {0}".format(args.form)

            locked_record_num = red_lock.lock_form(args.project, args.arm, event_desc, args.form, out_file = args.outfile, subject_id = args.subject_id)
            slog.takeTimer1("script_time","{'records': " + str(locked_record_num) + "}")
            if args.verbose:
                print "The {0} form has been locked".format(args.form)
                print "Record of locked files: {0}".format(args.outfile)

        elif args.unlock:
            if args.verbose:
                print "Attempting to unlock form: {0}".format(args.form)

            if not red_lock.unlock_form(args.project, args.arm, event_desc, args.form, subject_id = args.subject_id):
                print "Warning: Nothing to unlock ! Form '{0}' might not exist".format(args.form)
            elif args.verbose:
                print "The {0} form has been unlocked".format(args.form)

        elif args.report:
            form_array = args.form.split(",")
            if args.verbose:
                print "Attempting to create a report for form {0} and subject_id {1} ".format(form_array,args.subject_id)
            print red_lock.report_locked_forms(args.subject_id, args.subject_id, form_array, args.project, args.arm, event_desc)

        else :
            raise Exception("Please specify --lock, --unlock, or --report!")
            
    if args.verbose:
        print "Done!"

    slog.takeTimer1("script_time")


if __name__ == "__main__":
    formatter = argparse.RawDescriptionHelpFormatter
    default = 'default: %(default)s'
    parser = argparse.ArgumentParser(prog="redcap_form_locker.py",
                                     description=__doc__,
                                     formatter_class=formatter)
    parser.add_argument("--project", dest="project", required=False,
                        help="Project Name in lowercase_underscore.", default='ncanda_subject_visit_log')
    parser.add_argument("-a", "--arm", dest="arm", required=False,
                        choices=['Standard Protocol'],default='Standard Protocol',
                        help="Arm Name as appears in UI")
    parser.add_argument("-e", "--event", dest="event", required=False,
                        choices=['Baseline visit', '1y visit', '2y visit'],
                        help="Event Name in as appears in UI seperated with comma (if multiple ones)", default='Baseline visit,1y visit,2y visit')
    parser.add_argument("-f", "--form", dest="form", required=True,
                        help="Form Name in lowercase_underscore")
    parser.add_argument("-o", "--outfile", dest="outfile",
                        default='/tmp/locked_records.csv',
                        help="Path to write locked records file. {0}".format(default))
    parser.add_argument("-s", "--subject_id", default=None, help="REDCap subject ID")
    parser.add_argument("--lock", dest="lock", action="store_true",
                        help="Lock form")
    parser.add_argument("--unlock", dest="unlock", action="store_true",
                        help="Lock forms")
    parser.add_argument("--report", dest="report", action="store_true",
                        help="Generate a report for subject")

    parser.add_argument("-v", "--verbose", dest="verbose",
                        help="Turn on verbose", action='store_true')
    parser.add_argument("-p", "--post-to-github", help="Post all issues to GitHub instead of std out.", action="store_true")
    parser.add_argument("-t","--time-log-dir", help="If set then time logs are written to that directory", action="store", default=None)

    args = parser.parse_args()

    slog.init_log(args.verbose, args.post_to_github,'NCANDA REDCap', 'redcap_form_locker', args.time_log_dir)

    sys.exit(main(args=args))

