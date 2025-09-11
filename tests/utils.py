#########################
## TEST FIXTURE HELPERS
#########################

def get_session(config_file):
  '''
  Creates a Session from the provided configuration file.
  '''
  import sibispy.session as sess
  
  session = sess.Session()
  assert session.configure(config_file), "Configuration File `{}` is missing or not readable.".format(config_file)
  return session

def get_test_config(category, session):
  '''
  Returns the YAML category as a python object from the session's test configuration.
  '''
  parser, error = session.get_config_test_parser()
  assert error is None, "Error: getting test config: "+error
  return parser.get_category(category)