#!/usr/bin/env python

"""
Generalized mechanism for mass-setting a FIELD to a particular VALUE.

The CLI interface provides two APIs:

1. direct: For specified subjects within a given event, set FIELD to VALUE.

2. status: For the specific case of setting completeness and missignness, find
    those fields within the form and set them to the value, if specified.

"""

import argparse
import pandas as pd
import sys
import sibispy
from sibispy import sibislogger as slog


def parse_args(args=None):
    """
    Expose two APIs: direct/explicit for arbitrary field/value combinations,
    and status-based for completeness/missingness setting that infers variable
    names based on provided form name.

    args can be provided as a list of CLI items, e.g. ['-v', '--api',
    'data_entry']. This is useful for isolated testing.

    If args is None, argparse.parse_args(None) will automatically use sys.argv.
    """
    parser = argparse.ArgumentParser(description="",
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


def bulk_mark(redcap_api, field_name, value, records_df):
    """
    Workhorse bulk-marking function.

    NOTE: Could probably be moved to sibispy utils for reuse.
    """
    upload = records_df.copy(deep=True)
    upload[field_name] = value
    # TODO: Should probably wrap this in a try block, since we're not even
    # checking existence of variables?
    outcome = redcap_api.import_records(upload)
    
    return outcome


def get_status_fields_for_form(redcap_api, form_name):
    """
    Return completeness and (if available) missingness field names in a form.

    If form_name doesn't exist in the project data dictionary, raise NameError.

    Returns a dict with 'completeness' and 'missingness' keys.
    """
    datadict = redcap_api.export_metadata(format='df').reset_index()
    form_datadict = datadict.loc[datadict['form_name'] == form_name, :]
    if form_datadict.empty:
        raise NameError('{}: No such form in selected API!'.format(form_name))

    field_names = {'completeness': form_name + '_complete'}

    missing_field_name = form_datadict.loc[
            form_datadict['field_name'].str.endswith('_missing'),
            'field_name']  # FIXME: What type does this return?
    # TODO: Throw error if multiple fields are found?
    try:
        field_names.update({'missingness': missing_field_name.item()})
    except ValueError:  # no value available
        pass
    
    return field_names


def read_targets(redcap_api, from_file):
    """
    Convert file to a columnless DataFrame indexed by Redcap primary keys.

    If primary keys for the project are not present, raises AssertionError.
    """
    targets = pd.read_csv(from_file)
    out_cols = [redcap_api.def_field]
    assert redcap_api.def_field in targets.columns
    if redcap_api.is_longitudinal():
        assert 'redcap_event_name' in targets.columns
        out_cols.append('redcap_event_name')

    # If the file contains any other columns, strip them - don't want to add
    # them to the later upload
    targets = targets[out_cols]

    # Return a DataFrame with *only* appropriate indexing. This is necessary
    # for redcap.Project.import_records() to work properly once the variable of
    # interest is set for the DataFrame.
    #
    # (If multiple Redcap primary keys are standard columns, the MultiIndex is
    # wrongly assigned by .import_records() and the upload fails.)

    targets.set_index(out_cols, inplace=True)
    return targets


def bulk_mark_status(redcap_api, form_name, missingness, completeness,
        records_df, verbose=False):
    """
    Courtesy function to bulk-mark completeness and missingness. 
    
    Returns a tuple of import outcomes for completeness and missingness upload,
    respectively.

    Relies on argparse to provide valid completness and missingness values.
    """
    field_names = get_status_fields_for_form(redcap_api, form_name)
    comp_results = None
    miss_results = None

    # In either case, only set the status if it has been passed
    if completeness:
        comp_results = bulk_mark(redcap_api, field_names['completeness'],
                completeness, records_df)

    if missingness:
        if not field_names.get('missingness'):
            raise TypeError('Missingness cannot be set for selected form!')
        else:
            miss_results = bulk_mark(redcap_api, field_names['missingness'],
                    missingness, records_df)

    return (comp_results, miss_results)


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
        results = bulk_mark(api, args.field_name, args.value, targets)
        # TODO: Process results

    elif args.command == 'status':
        if args.missingness is None and args.completeness is None:
            sys.exit('One or both of missingness and completeness must be set!')
        else:
            if args.verbose:
                print(('Setting missingness to {missingness} and completion to'
                       ' {completeness} in {form}').format(**vars(args)))
            complete_results, missing_results = bulk_mark_status(api,
                    form_name=args.form, missingness=args.missingness,
                    completeness=args.completeness, records_df=targets)
            # TODO: Process results - the outcome is a dict with 'count' and
            # possibly 'error' keys
