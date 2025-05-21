"""
Generalized mechanism for mass-setting a FIELD to a particular VALUE.
"""
import pandas as pd
import redcap as rc
from six import string_types
from typing import Union, List, Dict


def bulk_mark(redcap_api: rc.Project, field_name: Union[List, str],
              value: str, records_df: pd.DataFrame, 
              upload_individually: bool = False) -> Dict:
    """
    Workhorse bulk-marking function.

    If applied to repeating instruments, `records_df` must already have valid
    `redcap_repeat_instrument` and `redcap_repeat_instance`.
    """
    upload = records_df.copy(deep=True)

    # upload.loc[:, field_name] = value
    if isinstance(field_name, string_types):
        assignments = {field_name: value}
    else:
        assignments = dict(zip(field_name, [value] * len(field_name)))

    # Might need to create multiple new columns with the same value, which
    # cannot be done with bare .loc if the columns don't already exist. This is
    # the simplest way to do it quickly. (If field_name is str, then this is
    # equivalent to upload.assign(field_name=value).)
    upload = upload.assign(**assignments)

    # TODO: Should probably wrap this in a try block, since we're not even
    # checking existence of variables and not uploading records one at a time?
    if upload_individually:
        outcomes = []
        for idx, _ in upload.iterrows():
            outcome = redcap_api.import_records(upload.loc[[idx]])
            outcomes.append(outcome)
    else:
        outcomes = redcap_api.import_records(upload)
    
    return outcomes


def get_status_fields_for_form(redcap_api, form_name):
    """
    Return completeness and (if available) missingness field names in a form.

    If form_name doesn't exist in the project data dictionary, raise NameError.

    Returns a dict with 'completeness' and 'missingness' keys.
    """
    datadict = redcap_api.export_metadata(format_type='df').reset_index()
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
    index_cols = [redcap_api.def_field]
    assert redcap_api.def_field in targets.columns
    if redcap_api.is_longitudinal():
        assert 'redcap_event_name' in targets.columns
        index_cols.append('redcap_event_name')

    out_cols = index_cols.copy()
    if 'redcap_repeat_instrument' in targets.columns:
        out_cols.extend(['redcap_repeat_instrument', 'redcap_repeat_instance'])

    # If the file contains any other columns, strip them - don't want to add
    # them to the later upload
    targets = targets[out_cols].drop_duplicates()

    # Return a DataFrame with *only* appropriate indexing. This is necessary
    # for redcap.Project.import_records() to work properly once the variable of
    # interest is set for the DataFrame.
    #
    # (If multiple Redcap primary keys are standard columns, the MultiIndex is
    # wrongly assigned by .import_records() and the upload fails.)

    targets.set_index(index_cols, inplace=True)
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
    if completeness is not None:
        comp_results = bulk_mark(redcap_api, field_names['completeness'],
                completeness, records_df)

    if missingness is not None:
        if not field_names.get('missingness'):
            raise TypeError('Missingness cannot be set for selected form!')
        else:
            miss_results = bulk_mark(redcap_api, field_names['missingness'],
                    missingness, records_df)

    return (comp_results, miss_results)
