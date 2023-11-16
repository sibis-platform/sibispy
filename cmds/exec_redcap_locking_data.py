#!/usr/bin/env python
from __future__ import print_function
from builtins import str
import sys
import argparse
import sibispy
from sibispy import cli
from sibispy import sibislogger as slog
from sibispy import redcap_locking_data 
import pandas as pd

from sqlalchemy import create_engine, MetaData, Table, select

def get_site_subjs_from_sql(session, engine, site):
    """
    Using the maria db connection to redcap, pull all records based on
    given site to get a list of subject for given data access group.
    """
    site = site[0].upper()

    # Create a connection and execute a query
    with engine.connect() as connection:
        # Build and execute the query to select all values from the table
        query = """
            SELECT * FROM 
                redcap_data as t1
            INNER JOIN
                redcap_data_access_groups as t2
            ON
                t1.field_name = "__GROUPID__" and t1.value = t2.group_id
            WHERE 
                t1.project_id = 20;
        """
        result = connection.execute(query)

        # Fetch all rows from the result set
        results = result.fetchall()

    df = pd.DataFrame(results, columns=['project_id', 'event_id', 'record', 'field_name', 'value', 'instance', 'group_id', 'project_id2', 'site'])
    # drop unneeded columns and duplicates
    df = df[['record', 'site']].drop_duplicates()
    # store only the site from argument
    df = df[df['site'].str.upper() == site].reset_index()

    # convert to a list of subjects
    subject_list = list(df['record'])

    return subject_list

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
        #TODO: Add site specific args
        if args.site:
            subject_list = get_site_subjs_from_sql(session, engine, args.site)
        else:
            subject_list = [None]
    else:
        print("INFO: kp: I do not think it works if subjects are defined")

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

    # for multiple events  simply seperate with space e.g. Baseline 1y"
    cli.add_event_param(parser, required=True, template="{} visit",
                        # backwards-compatible with the old '4y visit':
                        accepted_regex=r'^(Baseline|\dy|\dm)$', keep_nonmatch=True)
    cli.add_form_param(parser, dest='form', raise_missing=False, required=True,
                       short_switch='-f')
    cli.add_subject_param(parser, dest="subject_id")
    # TODO: can add handling of multiple sites in the cli prompt
    cli.add_site_param(parser, dest="site")
    cli.add_standard_params(parser)  # -v, -p, -t

    parser.add_argument("-o", "--outfile", dest="outfile",
                        default='/tmp/locked_records.csv',
                        help="Path to scratch-write current locked records file. {0}".format(default))
    action_group = parser.add_argument_group('Action parameters', '(Mutually exclusive)')
    action_group_exclusives = action_group.add_mutually_exclusive_group(required=True)
    action_group_exclusives.add_argument("--lock", dest="lock", action="store_true", help="Lock form(s)")
    action_group_exclusives.add_argument("--unlock", dest="unlock", action="store_true", help="Unlock form(s)")
    action_group_exclusives.add_argument("--report", dest="report", action="store_true", help="Generate a report of form lock statuses")

    args = parser.parse_args()

    slog.init_log(args.verbose, args.post_to_github, 'NCANDA REDCap', __file__, args.time_log_dir)

    sys.exit(main(args=args))

