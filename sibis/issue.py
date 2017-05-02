##
##  See COPYING file distributed along with the package for the copyright and
##  license terms
##
"""Create an issue on GitHub that uses a tag corresponding to the script any
error was detected with.
"""
from github.GithubException import UnknownObjectException


class Issue(object):
    """Reports an issue to GitHub using a custom format.

    Args:
        repo (object): An instance of a github.Repository object.
    """
    def __init__(self, repo):
        self.repo = repo

    def get_label(self, title, verbose=None):
        """Get a label object to tag the issue.

        Args:
            title (str): Title of posting.
            verbose (bool): True turns on verbose.

        Returns:
            An instance of github.Label.

        """
        if verbose:
            print "Checking for label..."
        label = None
        label_text = None
        try:
            label_start = 1 + title.index('(')
            label_end = title.index(')')
            label_text = title[label_start:label_end]
        except ValueError, e:
            print "Warning: This tile has no embeded label. {0}".format(e)
        if label_text:
            try:
                label = [self.repo.get_label(label_text)]
                if verbose:
                    print "Found label: {0}".format(label)
            except UnknownObjectException, e:
                print "Error: The label '{0}' does not exist on " \
                      "Github. {1}".format(label_text, e)
        return label

    def is_open_issue(self, subject, verbose=None):
        """Verify if issue already exists, if the issue is closed, reopen it.

        Args:
            subject (str): Subject line.
            verbose (bool): True turns on verbose.

        Returns:
            True if issue is already open.
        """
        if verbose:
            print "Checking for open issue: {0}".format(subject)
        for issue in self.repo.get_issues(state='all'):
            if issue.title == subject and issue.state == 'open':
                if verbose:
                    print "Open issue already exists... See: {0}".format(
                        issue.url)
                return True
            if issue.title == subject and issue.state == 'closed':
                if verbose:
                    print "Closed issue already exists, reopening... " \
                          "See: {0}".format(issue.url)
                issue.edit(state='open')
                return True
        if verbose:
            print "Issue does not already exist... Creating.".format(subject)
        return False

    def create_issue(self, title, issue, verbose=None):
        """Create a GitHub issue for the provided repository with a label

        Args:
            title (str): Contains label on github in parentheses.
            issue (dict): json formatted issue from sibis.logger.Logging
            verbose (bool): True turns on verbose

        Returns:
            None
        """
        # Validate title.
        if len(title) >= 254:
            title = title[:254]
        label = self.get_label(title)
        if not label:
            err = "A label embedded in parentheses is currently required. For "\
                  "example 'Title of Error (title_tag).' You provided: {0}"
            raise NotImplementedError(err.format(title))

        # Handle empty body
        if not issue:
            raise RuntimeWarning("The dict is empty and no issue will be "
                                 "created for file: {}.".format(issue))
        # Prepare issue for posting.
        issue.update({'title': title})
        error_msg = issue.get('error')
        issue_id = issue.get('issue_id')
        subject = "{}, {}".format(issue_id, error_msg)

        # Generate markdown for issue.
        body = "### {}\n".format(issue.pop('title'))
        for k, v in issue.iteritems():
            body += "- {}: {}\n".format(k, v)

        if self.is_open_issue(subject, verbose=verbose):
            # TODO: Post comment that issue is still present.
            pass
        else:
            github_issue = self.repo.create_issue(subject,
                                                  body=body,
                                                  labels=label)
            if verbose:
                print "Created issue... See: {0}".format(github_issue.url)
        return None
