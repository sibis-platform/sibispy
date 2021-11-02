###############################################################################
## Lifted from pyxnat and ported to Py3
###############################################################################

# This software is OSI Certified Open Source Software. OSI Certified is a 
# certification mark of the Open Source Initiative.

# Copyright (c) 2010-2011, Yannick Schwartz All rights reserved.

# Redistribution and use in source and binary forms, with or without modification, 
# are permitted provided that the following conditions are met:

#     1. Redistributions of source code must retain the above copyright notice, 
#        this list of conditions and the following disclaimer.

#     2. Redistributions in binary form must reproduce the above copyright notice, 
#        this list of conditions and the following disclaimer in the documentation 
#        and/or other materials provided with the distribution.

#     3. Neither the name of Yannick Schwartz. nor the names of other pyxnat 
#        contributors may be used to endorse or promote products derived from this 
#        software without specific prior written permission.

# This software is provided by the copyright holders and contributors "as is" and
# any express or implied warranties, including, but not limited to, the implied
# warranties of merchantability and fitness for a particular purpose are disclaimed.
# In no event shall the copyright owner or contributors be liable for any direct,
# indirect, incidental, special, exemplary, or consequential damages (including, but
# not limited to, procurement of substitute goods or services; loss of use, data, or
# profits; or business interruption) however caused and on any theory of liability,
# whether in contract, strict liability, or tort (including negligence or otherwise)
# arising in any way out of the use of this software, even if advised of the
# possibility of such damage.

def uri_parent(uri):
    # parent = uri

    # if not os.path.split(uri)[1] in resources_types:
    #     while os.path.split(parent)[1] not in resources_types:
    #         parent = os.path.split(parent)[0]

    #     return parent

    # support files in a hierarchy by stripping all but one level
    files_index = uri.find('/files/')
    if files_index >= 0:
        uri = uri[:7+files_index]
    return uri_split(uri)[0]

def uri_split(uri):
    return uri.rsplit('/', 1)

def uri_grandparent(uri):
    return uri_parent(uri_parent(uri))