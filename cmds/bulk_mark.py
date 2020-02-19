#!/usr/bin/env python

"""
The CLI interface to the generalized mechanism for mass-setting a FIELD to 
a particular VALUE.

Two interfaces are provided:

1. direct: For specified subjects within a given event, set FIELD to VALUE.

2. status: For the specific case of setting completeness and missignness, find
    those fields within the form and set them to the value, if specified.

"""

import argparse
import pandas as pd
import sys
import sibispy
from sibispy import sibislogger as slog
from sibispy.bulk_operations import (
    bulk_mark, get_status_fields_for_form, bulk_mark_status, read_targets)
import pdb 

def parse_args(args=None):
    """
    Expose two APIs: direct/explicit for arbitrary field/value combinations,
    and status-based for completeness/missingness setting that infers variable
    names based on provided form name.

    args can be provided as a list of CLI items, e.g. ['-v', '--api',
    'data_entry']. This is useful for isolated testing.

    If args is None, argparse.parse_args(None) will automatically use sys.argv.
    """
    parser = argparse.ArgumentParser(
        usage=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # Shared arguments
    parser.add_argument('--from-file',
            help="File with subject (and event) identifiers")
    parser.add_argument('--api',
            required=True,
            choices=['data_entry', 'import_laptops'],
            help="Name of sibispy-configured API to use.")
    parser.add_argument("-v", "--verbose",
            help="Verbose operation",
            action="store_true")

    # Two approaches to bulk setting
    subparsers = parser.add_subparsers(help="Bulk-setting method", 
            # Ensure that the used subparser is available in args.command
            dest='command')

    # 1. Define status and form to apply, let the command figure out field name and value
    status = subparsers.add_parser('status',
            help="Set missingness / completeneses status for a given form")
    status.add_argument('--form', required=True,
            help="Form to apply the status arguments to")

    completeness_status = status.add_mutually_exclusive_group()
    completeness_status.add_argument('--incomplete',
            dest='completeness', const=0, action='store_const')
    completeness_status.add_argument('--unverified',
            dest='completeness', const=1, action='store_const')
    completeness_status.add_argument('--complete',
            dest='completeness', const=2, action='store_const')

    missingness_status = status.add_mutually_exclusive_group()
    missingness_status.add_argument('--missing',
            dest='missingness', const=1, action='store_const')
    missingness_status.add_argument('--present', '--not-missing',
            dest='missingness', const=0, action='store_const')

    # 2. Specify explicitly field name and value, set that
    direct = subparsers.add_parser('direct',
            help="Manually specify field name and value")
    direct.add_argument('--field', '--field-name',
            help="Name of field to set")
    direct.add_argument('--value',
            help="Value to set in field")

    return parser.parse_args(args)


if __name__ == '__main__':
    # Set up the environment
    args = parse_args()
    if not args.command:
        sys.exit('You must specify "direct" or "status" handler!')
    else:
        slog.init_log(args.verbose, None, 'bulk_mark', 'bulk_mark', None)
        session = sibispy.Session()
        if not session.configure():
            sys.exit("Error: session configure file was not found")

        api = session.connect_server(args.api, True)
        targets = read_targets(api, args.from_file)

    # Based on selected handler, set the value
    if args.command == 'direct':
        if args.verbose:
            print('Setting {field} to {value} in {api}'.format(**vars(args)))
        results = bulk_mark(api, args.field, args.value, targets)
        # TODO: Process results

    elif args.command == 'status':
        if (args.missingness is None) and (args.completeness is None):
            sys.exit('One or both of missingness and completeness must be set!')
        else:
            if args.verbose:
                if args.missingness is not None:
                    print(('Setting missingness to {missingness} in {form}')
                          .format(**vars(args)))
                if args.completeness is not None:
                    print(('Setting completeness to {completeness} in {form}')
                          .format(**vars(args)))
            complete_results, missing_results = bulk_mark_status(api,
                    form_name=args.form, missingness=args.missingness,
                    completeness=args.completeness, records_df=targets)
            # TODO: Process results - the outcome is a dict with 'count' and
            # possibly 'error' keys

            if args.verbose:
                if complete_results is not None:
                    print("Set completeness for {} subjects".format(
                        complete_results.get('count')))
                if missing_results is not None:
                    print("Set missingness for {} subjects".format(
                        missing_results.get('count')))
