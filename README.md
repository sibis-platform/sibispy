# SIBIS
[![Circle CI](https://circleci.com/gh/sibis-platform/sibis.svg?style=svg)](https://circleci.com/gh/sibis-platform/sibis) [![Documentation Status](https://readthedocs.org/projects/sibis/badge/?version=latest)](http://sibis.readthedocs.org/en/latest/?badge=latest)

## Scalable Informatics for Biomedical Imaging Studies

SIBIS is python middleware for supporting clinical data management activities. It provides a common session object with access to imaging, form, and issue databases.

Contents
--------
* Install sibis-docker 

### Getting Started
To use `sibis`you need to have access to an XNAT server and a REDCap
server.

```python
import sibis

# Create a Session with configuration
session = sibis.Session(config_path="/path/to/config.yml")

# Use the logger for structured log format
# session.logging.info('id', 'err', **kwargs)

# Connect to the configured servers
session.connect_servers()

# Access the XNAT API
projects = session.api_imaging.select.projects()

# Access the REDCap API
data_entry = session.api_data_entry.export_records(format='df')
import_laptops = session.api_import_laptops_.export_records(format='df')
```

### Related Projects
- ncanda-data-integration: https://github.com/sibis-platform/ncanda-data-integration

