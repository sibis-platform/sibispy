import os, glob, stat, sys, imp

class SummaryScoresCollector():
  def __init__(self, module_init_script):

    self.fields_list = dict()
    self.functions = dict()
    self.output_form = dict()
    self.instrument_list = []

    module_dir = os.path.dirname(os.path.abspath(module_init_script))

    instruments = [ os.path.basename( d ) for d in glob.glob(os.path.join(module_dir,'*')) if stat.S_ISDIR( os.stat( d ).st_mode ) and os.path.exists( os.path.join( d, '__init__.py' ) ) ]

    sys.path.append( os.path.abspath(os.path.dirname(module_init_script) ) )

    for i in instruments:
      module_found = imp.find_module( i, [module_dir] )
      module = imp.load_module( i, module_found[0], module_found[1], module_found[2] )

      self.instrument_list.append( i )
      self.fields_list[i] =  module.input_fields
      self.functions[i] = module.compute_scores
      self.output_form[i] = module.output_form

  # dataframe and errorFlag
  def compute_scores(self, instrument, input_data, demographics):
    scoresDF = self.functions[instrument](input_data, demographics) 
    
    # remove nan entries as they corrupt data ingest (REDCAP cannot handle it correctly) and superfluous zeros
    # this gave an error as it only works for float values to replace
    if len(scoresDF):
      # Only execute it not empty 
      return (scoresDF.astype(object).fillna(''), False)
      
    return (scoresDF, False)
