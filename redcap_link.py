""" Specification:

    - There will **always** be an ID for the record. Everything else is
    optional.
    - Redcap project must be determined either by direct passage, or through
    inference.
    - (XNAT URLs might be supported under this schema, but that support may
    be dropped at any time.)
    - Unless arm is explicitly passed, it will be assumed to be the main
    one.
"""
import re
import yaml
import urllib

# TODO: 
# 1. Test the LinkGenerator
# 2. Include this in the logger
class RedcapLinkGenerator:
    def __init__(self, url_config=None,
                 url_config_file=None):
        if not url_config:
            if url_config_file:
                with open(url_config_file, 'r') as f:
                    self.config = yaml.load(f)
            else:
                raise ValueError("Cannot generate links without an URL config")
        else:
            self.config = url_config
        # return self.make_link

    def make_link(self, **kwargs):
        return RedcapLink(url_config=self.config, **kwargs)

class RedcapLink(object):
    def __init__(self, record_id, redcap_project=None, form=None,
                 event=None, arm=None, url_config=None):
        self.record_id = record_id

        if redcap_project is None:
            self.infer_redcap_project_from_id(self.record_id)
        else:
            self.redcap_project = redcap_project

        # TODO: Setter should check that these are available in the given Redcap project
        self.form = form
        self.event = event
        if arm is not None:
            self.arm = arm
        else:
            self.arm = "__default"

        # TODO: Project-wide level of getting at this info?
        self.config = url_config
        # self.load_url_configuration(url_config)

    @property
    def url(self):
        params = { 
            "id": self.record_id,
            "arm": self.get_arm_slug(),
            "page": self.form,
            "event": self.get_event_slug()
        }

        params = {k: v for k, v in params.items() if v}

        # If a form is provided, the dashboard page does
        # not automatically redirect to form-specific
        # page
        if "page" in params:
            link = self.get_base_url()
        else:
            link = self.get_dashboard_url()

        if not link:
            return

        if len(params):
            link = link + "&" + urllib.urlencode(params)

        return link

    @property
    def label(self):
        if self.form:
            if self.event:
                return "{}, {} ({})".format(self.record_id,
                                            self.form,
                                            self.event)
            elif not self.has_events():
                return "{}, {}".format(self.record_id,
                                       self.form)
        elif self.arm and (self.arm != "__default"):
                return "{}, {} arm".format(self.record_id,
                                           self.arm)

        # Fallback option:
        return "{}".format(self.record_id)

    @property
    def markdown_link(self):
        return "[" + self.label + "](" + self.url + ")"

    def __str__(self):
        if self.url:
            return self.markdown_link
        else:
            return self.label

    def load_url_configuration(self, yaml_file=None):
        if yaml_file:
            self.config = yaml.load(file(yaml_file, 'r'))
        else:
            # TODO: Raise?
            self.config = None

    def get_base_url(self):
        return (self.config
                .get(self.redcap_project, {})
                .get("base_url", ""))
    def get_dashboard_url(self):
        return (self.config
                .get(self.redcap_project, {})
                .get("dashboard_url", ""))

    def get_arm_slug(self):
        return (self.config
                .get(self.redcap_project, {})
                .get("arms", {})
                .get(self.arm, {})
                .get("arm_id", ""))

    def has_events(self):
        return (self.config
                .get(self.redcap_project, {})
                .get("arms", {})
                .get(self.arm, {})
                .get("events")) is not None

    def get_event_slug(self):
        return (self.config
                .get(self.redcap_project, {})
                .get("arms", {})
                .get(self.arm, {})
                .get("events", {})
                .get(self.event, ""))

    def infer_arm(self, redcap_project=None, form=None, event=None):
        if self.event is not None:
            pass
        if self.form is not None:
            pass
            # get the FEM
        pass

    def infer_redcap_project_from_id(self, id=None):
        if id is None:
            id = self.record_id

        if re.search("^[A-E]-[0-9]{5}-[MF]-[0-9]$", id):
            self.redcap_project = "data_entry"
        # elif re.search("^NCANDA_[SE]", id):
        #     self.redcap_project = "xnat"
        else:
            self.redcap_project = "import_laptops"

def infer_redcap_project(df_row):
    columns = df_row.index.tolist()
    if "record_id" in columns:
        return "import"
    elif "study_id" in columns:
        return "entry"
    pass

def create_link_from_row(df_row):
    pass
