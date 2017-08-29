#!/usr/bin/env python

##
##  See COPYING file distributed along with the ncanda-data-integration package
##  for the copyright and license terms
##
"""
Post GitHub Issues
------------------

Take the stdout and stderr passed to the catch_output_email and create an issue
on GitHub that uses a tag corresponding to the script any error was detected
with.

Example Usage:

python post_github_issues.py -o sibis-platform -r ncanda-issues \
                             -t "NCANDA: Laptop Data (update_visit_data)" \
                             -b /tmp/test.txt -v
"""
import os
import sys
import json
import hashlib

import github
from github.GithubException import UnknownObjectException, GithubException

from sibispy import config_file_parser as cfg_parser

#def format_error_message(errmsg,issue): 
#    sha = hashlib.sha1(errmsg + issue).hexdigest()[0:6]
#    error_dict = dict(experiment_site_id="Error:{}".format(sha),
#                      error=issue,
#                      error_msg=err_msg)
#    return generate_body(error_dict)

def ping_github():
    # 0 means everything is working well
    return os.system("ping -c 1 www.github.com > /dev/null")
        
def get_github_label(repo, label_text, verbose=None):
    """Checks if the label is valid

    Args:
        repo (object): A github.Repository object.
        label (str): Title of posting.
        verbose (bool): True turns on verbose.

    Returns:
        object: A github.Label.

    """

    label = None
    if not repo :
        raise ValueError("Error:post_issues_to_github: repo is not defined for label '" + label_text +"'") 
        
    if label_text:
        try:
            label = [repo.get_label(label_text)]
            if verbose:
                print "Found label: {0}".format(label)
        except UnknownObjectException, e:
            raise ValueError("Error:post_issues_to_github: The label '{0}' does not exist on Github. {1}".format(label_text, e))

    return label


def get_github_label_from_title(repo, title, verbose=None):
    """Get a label object to tag the issue.

    Args:
        repo (object): A github.Repository object.
        title (str): Title of posting.
        verbose (bool): True turns on verbose.

    Returns:
        object: A github.Label.

    """
    if verbose:
        print "Checking for label..."

    label_text = None
    try:
        label_start = 1 + title.index('(')
        label_end = title.index(')')
        label_text = title[label_start:label_end]
    except ValueError, e:
        print "Warning: This tile '" + title + "' has no embeded label. "
        print "  A label embedded in parentheses is currently required. For " 
        print "  example 'Title of Error (title_tag).' You provided: " + title
        print "  The following error message was produced when trying to extract label:"
        print str(e)
        return None

    label = None
    try :  
        label = get_github_label(repo,label_text,verbose)
    except ValueError, err_msg:
        print "Error:post_issues_to_github: Could not get label for tile '" + title + "!'"
        print err_msg
        
    return label
    
    
def get_issue(repo, subject, labelList = None, verbose=None):
    """get issue it it already exists.

    Args:
        repo (object): a github.Repository.
        subject (str): Subject line.
        verbose (bool): True turns on verbose.

    Returns:
        github.Issue.Issue
    """
    if not repo :
        raise ValueError("Error:post_issues_to_github: repo is not defined for subject '" +  subject +"'") 
 
    if verbose:
        print "Checking for issue: {0}".format(subject)

    if labelList: 
        issueList = repo.get_issues(state='all',labels=labelList)
    else : 
        issueList = repo.get_issues(state='all')

    for issue in issueList: 
        if issue.title == subject :
            return issue

    return None

def is_open_issue(repo, subject, label=None, verbose=None):
    """Verify if issue already exists, if the issue is closed, reopen it.

    Args:
        repo (object): a github.Repository.
        subject (str): Subject line.
        verbose (bool): True turns on verbose.

    Returns:
        bool: True if issue is already open.
    """

    issue = get_issue(repo, subject, labelList=label, verbose=verbose)

    if issue:
        if issue.state == 'open':
            if verbose:
                print "Open issue already exists... See: {0}".format(issue.url)
            return True

        if verbose:
            print "Closed issue already exists, reopening... " \
                "See: {0}".format(issue.url)
        try:
            issue.edit(state='open')
        except GithubException as error:
            print("Error:post_issues_to_github: Edit open issue failed for subject ({}), title ({}): {}".format(subject, issue.title, error))
        return True

    if verbose:
        print "Issue does not already exist... Creating.".format(subject)

    return False

def generate_body(issue):
    """Generate Markdown for body of issue.

    Args:
        issue (dict): Keys for title and others.

    Returns:
        str: Markdown text.
    """
    markdown = "### {}\n".format(issue.pop('title'))
    for k, v in issue.iteritems():
        markdown += "- {}: {}\n".format(k, v)
    return markdown


def get_valid_title(title):
    """Ensure that the title isn't over 255 chars.

    Args:
        title (str): Title to be used in issue report.

    Returns:
        str: Less than 255 chars long.
    """
    if len(title) >= 254:
        title = title[:254]
    return title

def create_issues_from_list(repo, title, label, issue_list, verbose=None):
    """Create a GitHub issue for the provided repository with a label

    Args:
        repo: github.Repository
        title (str): General title to be used to post issues 
        label (str): github label 
        issue_list (list): list of issues    
        verbose (bool): True turns on verbose

    Returns:
        None
    """

    if not issue_list or not label:
        return None

    # Handle multiline error messages.
    if 'Traceback' in ''.join(issue_list):
        if verbose:
            print "Issue is a Traceback..."
        string = "".join(issue_list)
        sha = hashlib.sha1(string).hexdigest()[0:6]
        error = dict(experiment_site_id="Traceback:{}".format(sha),
                     error="Traceback",
                     message=string)
        issue_list = [json.dumps(error, sort_keys=True)]

    for issue in issue_list:
        # Check for new format
        try:
            issue_dict = json.loads(issue)
            issue_dict.update({'title': get_valid_title(title)})
            error_msg = issue_dict.get('error')
            experiment_site_id = issue_dict.get('experiment_site_id')
            subject = "{}, {}".format(experiment_site_id, error_msg)
            body = generate_body(issue_dict)
        except:
            if verbose:
                print("Falling back to old issue formatting.")
            # Old error handling approach.
            # Create a unique id.
            sha1 = hashlib.sha1(issue).hexdigest()[0:6]
            subject_base = title[0:title.index(' (')]
            subject = subject_base + ": {0}".format(sha1)
            body = issue
        try:
            open_issue = is_open_issue(repo, subject, label = label, verbose=verbose)

        except Exception as e:
            print 'Error:post_issues_to_github: Failed to check for open issue on github!' + ' Title: ' +  subject + ", Exception: " + str(e)
            pass
        else:
            if open_issue:
                pass
            else:
                try:
                    github_issue = repo.create_issue(subject, body=body, labels=label)
                except Exception as e:
                    print 'Error:post_github_issues: Failed to post the following issue on github!' + ' Title: ' + subject + ", Body: " + body + ", Exception: " + str(e)
                else:
                    if verbose:
                        print "Created issue... See: {0}".format(github_issue.url)
    return None

def get_issues_from_file(file_name, verbose=None):
    """get issues 

    Args:
        file_name (str):
        verbose (bool): True turns on verbose

    Returns:
        None
    """
    with open(file_name) as fi:
        issues = fi.readlines()
        fi.close()
    # Handle empty body
    if not issues:
        raise RuntimeWarning("The body text is empty and no issue will be "
                             "created for file: {}.".format(body))
    return issues


def connect_to_github(config_file=None,verbose=False): 
    if verbose:
        print "Setting up GitHub..."
        print "Parsing config: {0}".format(config_file)


    config_data = cfg_parser.config_file_parser()
    err_msg = config_data.configure(config_file)
    if err_msg:
        print "Error:post_issues_to_github: Reading config file " + config_file + " (parser tried reading: " + config_data.get_config_file() + ") failed: " + str(err_msg)

        return None

    user = config_data.get_value('github', 'user')
    passwd = config_data.get_value('github', 'password')
    org_name = config_data.get_value('github', 'org')
    repo_name = config_data.get_value('github', 'repo')
    if not user or not passwd or not org_name or not repo_name: 
        print "Error:post_issues_to_github: github definition is incomplete in " +  config_data.get_config_file()
        return None

    g = github.Github(user, passwd)

    if not g: 
        print "Error:post_issues_to_github: Could not connect to github repository as defined by " +  config_data.get_config_file()
        return None

    if verbose:
        print "Connected to GitHub"

    try :
        organization = g.get_organization(org_name)
    except Exception as e :
        print "Error:post_issues_to_github: getting organization (" + org_name + ") as defined in " + config_data.get_config_file() + " failed with error message: '" + str(e) + "' .  Pinging github (0=OK): " + str(ping_github())  
        return None

    try :
        repo = organization.get_repo(repo_name)
    except Exception as e :
        print "Error:post_issues_to_github: Getting repo (" + repo + ") as defined in " + config_data.get_config_file() + " failed with the following error message: " + str(e)
        return None

    if verbose:
        print "... ready!"


    return repo


def main(args=None):
    issue_list = get_issues_from_file(args.body, args.verbose)

    repo = connect_to_github(args.config,args.verbose)
    if not repo:
        print "Error:post_issues_to_github: For `" + str(args.title) + "` could not connect to github repo ! Info: The following issues were not posted/closed: " + str(issue_list)
        return 1

    label = get_github_label_from_title(repo, args.title)

    if not label:
        raise NotImplementedError('Label not implemented')

    if args.closeFlag : 
        if args.verbose:
            print "Closing issues!" 

        for issue in issue_list:
            # just copied from code above as I ran out of time - function should be called
            # this function is for debugging only right now so we should be fine
            try:
                issue_dict = json.loads(issue)
                issue_dict.update({'title': get_valid_title(args.title)})
                error_msg = issue_dict.get('error')
                experiment_site_id = issue_dict.get('experiment_site_id')
                subject = "{}, {}".format(experiment_site_id, error_msg)
            except:
                if args.verbose:
                    print("Falling back to old issue formatting.")
                # Old error handling approach.
                # Create a unique id.
                sha1 = hashlib.sha1(issue).hexdigest()[0:6]
                subject_base = title[0:args.title.index(' (')]
                subject = subject_base + ": {0}".format(sha1)

            git_issue= get_issue(repo, subject , label, False)

            if git_issue:
                print "Closing", str(issue) 
                try: 
                    git_issue.edit(state='close')
                except GithubException as error:
                    print("Error:post_issues_to_github: Closing issue failed for subject ({}), title ({}). {}".format(subject, issue.title, error))
                    raise RuntimeError('Github Server Problem')

            else :
                "Warning: Issue '" + str(issue) +"' does not exist!"  
    else :
        create_issues_from_list(repo, args.title, label, issue_list, args.verbose)

    if args.verbose:
        print "Finished!"

if __name__ == "__main__":
    import argparse

    formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(prog="post_github_issues.py",
                                     description=__doc__,
                                     formatter_class=formatter)
    parser.add_argument("-c", "--config", dest="config", help="File containing GitHub authentication info. If not defined then use default setting of config_file_parser.py")
    parser.add_argument("-t", "--title", dest="title", required=True,
                        help="GitHub issue title with label in parentheses.")
    parser.add_argument("-b", "--body", dest="body", required=True,
                        help="GitHub issue body.")
    parser.add_argument("-v", "--verbose", dest="verbose", action='store_true',
                        help="Turn on verbose.")
    parser.add_argument("--close", dest="closeFlag", action='store_true',
                        help="Close issues that are in a list.")
    argv = parser.parse_args()
    sys.exit(main(args=argv))
