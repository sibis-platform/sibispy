"""
A number of argparse helpers, generally using custom argument typing for
additional input value transformations.
"""
import argparse
import re
import sys
from typing import List, Tuple, Union


def add_standard_params(parser: argparse.ArgumentParser) -> None:
    """
    Adds -v, -p and -t.
    """
    parser.add_argument("-v", "--verbose",
                        dest="verbose",
                        help="Print debug info to stdout",
                        action='store_true')
    parser.add_argument("-p", "--post-to-github",
                        help="Post all issues to GitHub instead of stdout",
                        action="store_true")
    parser.add_argument("-t", "--time-log-dir",
                        help="If set, time logs are written to that directory",
                        action="store", default=None)


def add_event_param(
        parser: argparse.ArgumentParser,
        dest: str = "event",
        required: bool = False,
        template: str = "{}y visit",
        accepted_regex: str = r'^\d+$',
        keep_nonmatch: bool = False) -> None:
    """
    Handles and transforms arbitrarily entered events.
    """

    nargs = '+' if required else '*'

    def __event_handler(arg: str) -> Union[str, None]:
        if re.match(accepted_regex, arg):
            return template.format(arg)
        elif keep_nonmatch:
            return arg
        else:
            return None

    parser.add_argument("-e", "--event",
                        help=("Event name matching regex {}, before "
                              "transformation to {}").format(accepted_regex,
                                                             template),
                        nargs=nargs,
                        required=required,
                        type=__event_handler)


def add_subject_param(parser: argparse.ArgumentParser,
                      dest="subject",
                      required: bool = False,
                      choices: List[str] = None) -> None:
    """
    Add parameter for subject ID entry.
    """
    nargs = '+' if required else '*'

    parser.add_argument("-s", "--study-id", "--subject",
                        help="Subject IDs that the script should affect",
                        nargs=nargs,
                        dest=dest,
                        choices=choices,
                        # metavar="STUDY_ID"
                        required=required,
                        action="store")


def add_form_param(parser: argparse.ArgumentParser,
                   dest='forms',
                   required: bool = False,
                   eligible_forms: List[Tuple[str]] = [],
                   raise_missing: bool = True,
                   short_switch="-i") -> None:
    """
    Add the forms parameter, with checking for form aliases.

    eligible_forms: a list of tuples, where the first element in the tuple is
        the canonical name of the form, and the remaining are allowable names
        for the form
    short_switch: the "short" option - `-i` by default (short for instruments),
        but some CLIs use -f to mean --forms instead of --force.
    """
    # _eligible_forms = []
    # for x in eligible_forms:
    #     if isinstance(x, string_types):
    #         _eligible_forms.append((x))
    #     else:
    #         _eligible_forms.append(x)

    def __form_handler(arg):
        if len(eligible_forms) == 0:
            return arg

        forms_found = [x for x in eligible_forms if arg in x]
        if len(forms_found) == 1:
            return forms_found[0][0]
        elif not raise_missing:
            return None
        else:
            if len(forms_found) == 0:
                raise ValueError("{} not found in eligible forms {}"
                                 .format(arg, eligible_forms))
            elif len(forms_found) > 1:
                raise ValueError("Ambiguous {}, found in following forms: {}"
                                 .format(arg, forms_found))

    nargs = '+' if required else '*'
    parser.add_argument(short_switch, '--forms', '--form', '--instrument',
                        help="Forms that the script should affect",
                        dest=dest,
                        nargs=nargs,
                        required=required,
                        type=__form_handler)


def transform_commas_in_args(arg_list: List[str] = sys.argv[1:]) -> List[str]:
    """
    Essentially, turn comma splits into spaces.

    Limited, in that it splices *any* commas, which might be actually valid
    filename parts etc. Ideally, this would be part of an argparse.Action.
    """
    _arg_list = []
    for x in arg_list:
        if "," in x:
            _arg_list.extend(x.split(","))
        else:
            _arg_list.append(x)
    return _arg_list


# if __name__ == '__main__':
#     args = transform_commas_in_args(sys.argv[1:])
#     parser = argparse.ArgumentParser()
#     add_event_param(parser, template="visit_{}")
#     add_subject_param(parser)
#     add_form_param(parser, eligible_forms=[
#         ('limesurvey_ssaga_youth', 'lssaga_youth', 'lssaga1_youth')
#     ])
#     add_standard_params(parser)
#     print(parser.parse_args(args))
