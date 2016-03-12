#!/usr/bin/env python
"""

"""
__author__ = 'Nolan Nichols <https://orcid.org/0000-0003-1099-3328>'
import os
import sys


def main(args=None):
    pass


if __name__ == "__main__":
    import argparse

    formatter = argparse.RawDescriptionHelpFormatter
    default = 'default: %(default)s'
    parser = argparse.ArgumentParser(prog="file_name.py",
                                     description=__doc__,
                                     formatter_class=formatter)
    argv = parser.parse_args()
    sys.exit(main(args=argv))
