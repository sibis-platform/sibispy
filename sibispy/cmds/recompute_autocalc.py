#!/usr/bin/env python
"""
Upload random values to non-repeating forms in order to force Redcap autocalc.

DO NOT USE THIS ON REPEATING FORMS.
"""
import argparse
import pdb
import sibispy
from sibispy import sibislogger as slog
from sibispy import bulk_operations as bulk
import random
import redcap as rc
from typing import Union, List, Dict

"""
NOTE: At this point, the script will set a random value on all records in the
Redcap project, **regardless of whether there's any data on the form for that
particular subject.**

NOTE: for some versions of Redcap (pre-8.10.3 and pre-9.0.0), a "bug" means
that a single edit anywhere on the event will recalculate all variables
therein:
https://community.projectredcap.org/questions/55192/when-are-calculated-fields-saved.html

From testing on 8.7.1, it also seems that any event where a calculation is
triggered will trigger recalculations on all events, which means we might get
away with doing this just on visit_notes - since all events calculate `age`.

For those cases, we can get by with a certain economy of updates.
"""


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        # usage=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    form_options = parser.add_mutually_exclusive_group()
    # Dropping --all-forms for now because we don't have a quick way of picking
    # just the non-repeating ones
    # form_options.add_argument('--all-forms', action="store_true")
    form_options.add_argument('--forms', nargs='*',
                              help="Non-repeating forms to refresh")

    parser.add_argument('-e', '--events', nargs='*', default=None,
                        help="Limit auto-generation to listed events")
    parser.add_argument('-T', '--template', default='{form}_update_aux_',
                        help="Template for inferring variable name from form")
    parser.add_argument("-v", "--verbose",
                        help="Verbose operation",
                        action="store_true")
    parser.add_argument('--api',
                        required=True,
                        help="Name of sibispy-configured API to use.")
    return parser.parse_args()


def _get_variable_names(api: rc.Project, forms: List, template: str,
                        verbose=False) -> List:
    varnames = [template.format(form=x) for x in forms]
    actual_varnames = [x for x in varnames if x in api.field_names]
    dropped_varnames = [x for x in varnames if x not in actual_varnames]
    if verbose:
        print("Setting random value on the following variables: "
              F"{actual_varnames}")
        if len(dropped_varnames) > 0:  # and not args.all_forms:
            print("The following variables don't exist in target API: "
                  F"{dropped_varnames}")
    return actual_varnames


def _get_valid_forms(api: rc.Project, forms: List, template: str) -> List:
    """
    Filter down passed forms based on their presence in the project + presence
    """
    valid_forms = []
    for form in forms:
        if form not in api.forms:
            continue
        elif template.format(form=form) not in api.field_names:
            continue
        else:
            valid_forms.append(form)

    return valid_forms


def _get_variable_names_by_form(api, forms: rc.Project, template: str,
                                verbose: bool = False) -> Dict[str, str]:
    """
    Useful for getting valid forms and their trash variables.

    NOTE: Not currently used.
    """
    valid_forms = _get_valid_forms(api, forms, template)
    varnames_by_form = dict(zip(
        valid_forms, _get_variable_names(api, valid_forms, template, verbose)))
    # varnames_by_form = {form: _get_variable_names(api, [form], template)
    #                     for form in forms}
    # varnames_by_form = {form: varname_list[0]
    #                     for form, varname_list
    #                     in varnames_by_form.items()
    #                     if len(varname_list) > 0}
    return varnames_by_form


def _get_forms(api: rc.Project, forms: List, events: List = None) -> List:
    """
    Given (presumably valid) forms, retrieve their _complete field to get a
    full listing of their content by subject.

    NOTE: Pre-requisite to getting things right with repeating instances.
    
    Not currently used.
    """
    form_dataframes = dict(zip(forms, [None] * len(forms)))
    for form in forms:
        form_complete = '{form}_complete'
        form_export = api.export_records(format='df',
                                         fields=[api.def_field, form_complete],
                                         events=events)
        # First, drop rows that don't contain data (which shouldn't be any);
        # then, drop any rows that don't apply (likely because they're
        # repeating-form metadata on a non-repeating form)
        form_export = (form_export
                       .dropna(axis=0)
                       .dropna(axis=1)
                       .drop(columns=[form_complete]))
        form_dataframes[form] = form_export
    return form_dataframes


def main():
    args = parse_args()
    slog.init_log(verbose=args.verbose,
                  post_to_github=False,
                  github_issue_title='Recomputing Redcap auto-calculations',
                  github_issue_label='bug',  # recompute_autocalc
                  timerDir=None)
    session = sibispy.Session(opt_api={args.api: None})
    if not session.configure():
        return None
    api = session.connect_server(args.api)
    if api is None:
        raise KeyError("Invalid API name {}!".format(args.api))

    # if args.all_forms:
    #     forms = _get_valid_forms(api, api.forms, args.template)
    # else:
    forms = _get_valid_forms(api, args.forms, args.template)

    # Filter down to only valid variable names
    varnames = _get_variable_names(api, forms, args.template, args.verbose)

    # Redcap needs an actual change -> generate a new random number every time
    random_value = random.randint(10**6, 10**7)
    targets = (api.export_records(fields=[api.def_field],
                                  events=args.events,
                                  format='df')
               .dropna(axis=1, how='all'))

    # FIXME: Doesn't handle repeating forms correctly. To do that, need to:
    #
    # 1. determine which forms have a trash field to be set
    # 2. export the _complete field for each form
    # 3. split up the delivered results by NA-ness of redcap_repeat_instance
    # 4. if redcap_repeat_instance.notnull(),
    # 5. proceed from there

    # NOTE: `upload_individually` here is necessary, or at least uploading in
    # small batches - from testing on 8.7.1, we found that if a Redcap upload
    # is sufficiently large, the auto-calculation will not happen.
    result = bulk.bulk_mark(api, varnames, random_value, targets,
                            upload_individually=True)
    print(result)


if __name__ == '__main__':
    main()
