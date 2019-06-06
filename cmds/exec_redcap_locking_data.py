#!/usr/bin/env python
from __future__ import print_function
from builtins import str
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
            print("Error: session configure file was not found")

        sys.exit(1)

    engine = session.connect_server('redcap_mysql_db', True)
    if not engine :
        if args.verbose:
            print("Error: Could not connect to REDCap mysql db")

        sys.exit(1)

    if args.verbose:
        print("Connected to REDCap using: {0}".format(engine))

    red_lock = redcap_locking_data.redcap_locking_data()
    red_lock.configure(session)
    

    if args.unlock and args.lock:
        raise Exception("Only specify --lock or --unlock, not both!")

    subject_list = args.subject_id
    if subject_list is None:
        subject_list = [None]


    for event_desc in args.event:
        if args.verbose: 
            print("Visit: {0}".format(event_desc))

        if args.lock:
            if args.verbose:
                print("Attempting to lock form: {0}".format(args.form))

            for sid in subject_list:
                for form in args.form:
                    locked_record_num = red_lock.lock_form(args.project, args.arm, event_desc, form, outfile=args.outfile, subject_id=sid)
                    slog.takeTimer1("script_time", "{'records': " + str(locked_record_num) + "}")
                    if args.verbose:
                        print("The {0} form has been locked".format(form))
                        print("Record of locked files: {0}".format(args.outfile))

        elif args.unlock:
            if args.verbose:
                print("Attempting to unlock form: {0}".format(args.form))
                
            for sid in subject_list:
                for form in args.form:
                    if not red_lock.unlock_form(args.project, args.arm, event_desc, form, subject_id=sid):
                        if sid:
                            print("Warning: Nothing to unlock! Form '{0}' or subject '{1}' might not exist".format(form, sid))
                        else:
                            print("Warning: Nothing to unlock! Form '{0}' might not exist".format(form))
                    elif args.verbose:
                        print("The {0} form has been unlocked".format(form))

        elif args.report:
            if not args.subject_id:
                raise NotImplementedError("Cannot create report if no subject ID is passed!")
            form_array = args.form
            if args.verbose:
                print("Attempting to create a report for form(s) {0} and subject_id {1} ".format(form_array,args.subject_id))
            for subject in args.subject_id:
                # FIXME: Currently, Session.get_mysql_table_records cannot take multiple subject IDs
                print(red_lock.report_locked_forms(subject, subject, form_array, args.project, args.arm, event_desc))

    if args.verbose:
        print("Done!")

    slog.takeTimer1("script_time")


if __name__ == "__main__":
    formatter = argparse.RawDescriptionHelpFormatter
    default = 'default: %(default)s'
    parser = argparse.ArgumentParser(prog=__file__,
                                     description=__doc__,
                                     formatter_class=formatter)
    parser.add_argument("--project", dest="project", required=False,
                        help="Project Name in lowercase_underscore.", default='ncanda_subject_visit_log')
    parser.add_argument("-a", "--arm", dest="arm", required=False,
                        choices=['Standard Protocol'], default='Standard Protocol',
                        help="Arm Name as appears in UI")
    parser.add_argument("-e", "--event", dest="event", required=False, nargs="*",
                        choices=['Baseline visit', '1y visit', '2y visit', '3y visit','4y visit'],
                        help="Event Name in as appears in UI", default=['Baseline visit', '1y visit', '2y visit', '3y visit', '4y visit'])
    parser.add_argument("-f", "--form", dest="form", required=True, nargs="+",
                        help="Form Name in lowercase_underscore")
    parser.add_argument("-o", "--outfile", dest="outfile",
                        default='/tmp/locked_records.csv',
                        help="Path to scratch-write current locked records file. {0}".format(default))
    parser.add_argument("-s", "--subject_id", help="REDCap subject ID(s) (separate with spaces)", nargs="*")
    action_group = parser.add_argument_group('Action parameters', '(Mutually exclusive)')
    action_group_exclusives = action_group.add_mutually_exclusive_group(required=True)
    action_group_exclusives.add_argument("--lock", dest="lock", action="store_true", help="Lock form(s)")
    action_group_exclusives.add_argument("--unlock", dest="unlock", action="store_true", help="Unlock form(s)")
    action_group_exclusives.add_argument("--report", dest="report", action="store_true", help="Generate a report of form lock statuses")

    parser.add_argument("-v", "--verbose", dest="verbose",
                        help="Turn on verbose", action='store_true')
    parser.add_argument("-p", "--post-to-github", help="Post all issues to GitHub instead of std out.", action="store_true")
    parser.add_argument("-t", "--time-log-dir", help="If set then time logs are written to that directory", action="store", default=None)

    args = parser.parse_args()

    slog.init_log(args.verbose, args.post_to_github, 'NCANDA REDCap', __file__, args.time_log_dir)

    sys.exit(main(args=args))

