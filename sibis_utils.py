# General Util functions
from sibispy import sibislogger as slog

date_format_ymd = '%Y-%m-%d'

# "Safe" CSV export - this will catch IO errors from trying to write to a file
# that is currently being read and will retry a number of times before giving
# up. This function will also confirm whether the newly created file is
# different from an already existing file of the same name. Only changed files
# will be updated.
def safe_dataframe_to_csv(df, fname, verbose=False):
    import pandas
    import time
    import filecmp
    import os

    success = False
    retries = 10

    while (not success) and (retries > 0):
        try:
            df.to_csv(fname + '.new', index=False)
            success = True
        except IOError as e:
            if e.errno == 11:
                if verbose : 
                    print "Failed to write to csv ! Retrying in 5s..."
                time.sleep(5)
                retries -= 1
            else:
                retries = 0

    if not success:
        slog.info("safe_dataframe_to_csv","ERROR: failed to write file" + str(fname) + "with errno" + str(e.errno))
        return False

    # Check if new file is equal to old file
    if os.path.exists(fname) and filecmp.cmp(fname, fname + '.new',shallow=False):
        # Equal - remove new file
        os.remove(fname + '.new')
    else:
        # Not equal or no old file: put new file in its final place
        os.rename(fname + '.new', fname)
        if verbose:
            print "Updated", fname

    return True
    

 
