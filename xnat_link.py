"""

Since XNAT POSTs its search terms and then redirects to a
stable URL if it gets a match, only a subset of XNAT links
that uses XNAT identifiers is available.

"""
import re

# TODO: Use config file
XNAT_SEARCH_URL = "https://ncanda.sri.com/xnat/app/action/DisplayItemAction/"
class XnatLink(object):
    def __init__(self, xnat_id=None):
        self.xnat_id = xnat_id
        self.infer_search_metadata()

    def infer_search_metadata(self):
        self.search_element = None
        self.search_field = None
        if self.xnat_id is None:
            return
        if re.search("^NCANDA_S[0-9]{4,5}$", self.xnat_id):
            self.search_element = "xnat:subjectData"
            self.search_field = "xnat:subjectData.ID"
        elif re.search("^NCANDA_E[0-9]{4,5}$", self.xnat_id):
            self.search_element = "xnat:mrSessionData"
            self.search_field = "xnat:mrSessionData.ID"
        return

    @property
    def label(self):
        return self.xnat_id

    @property
    def url(self):
        if self.search_element and self.search_field:
            return XNAT_SEARCH_URL + "search_value/{}/search_element/{}/search_field/{}".format(
                self.xnat_id,
                self.search_element,
                self.search_field
            )

    @property
    def markdown_link(self):
        return "[" + self.label + "](" + self.url + ")"

    def __str__(self):
        if self.url:
            return self.markdown_link
        else:
            return self.label
