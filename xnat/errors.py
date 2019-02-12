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

from builtins import str
import re

from lxml import etree

# parsing functions


def is_xnat_error(msg):
    if isinstance(msg, bytes):
        message = msg.decode('utf-8')
    else:
        message = msg
    return message.startswith('<!DOCTYPE') or message.startswith('<html>')


def parse_error_message(msg):
    try:
        if isinstance(msg, bytes):
            message = msg.decode('utf-8')
        else:
            message = msg
            
        if message.startswith('<html>'):
            message_tree = etree.XML(message)
            error_tag = message_tree.find('.//h3')
            if error_tag:
                error = error_tag.xpath("string()")
        elif message.startswith('<!DOCTYPE'):
            message_tree = etree.XML(message)
            error_tag = message_tree.find('.//title')
            if error_tag and 'Not Found' in error_tag.xpath("string()"):
                error = error_tag.xpath("string()")
            else:
                error_tag = message_tree.find('.//h1')
                if error_tag:
                    error = error_tag.xpath("string()")
        else:
            error = message

    except Exception:
        error = message
    finally:
        return error


def parse_put_error_message(message):
    error = parse_error_message(message)

    required_fields = []

    if error:
        for line in error.split('\n'):

            try:
                datatype_name = re.findall("\'.*?\'", line)[0].strip('\'')
                element_name = re.findall("\'.*?\'", line
                                          )[1].rsplit(':', 1)[1].strip('}\'')

                required_fields.append((datatype_name, element_name))
            except:
                continue

    return required_fields


def catch_error(msg_or_exception, full_response=None):

    # handle errors returned by the xnat server
    if isinstance(msg_or_exception, str):
        # parse the message
        msg = msg_or_exception
        error = parse_error_message(msg)

        # choose the exception
        if error == 'The request requires user authentication':
            raise OperationalError('Authentication failed')
        elif 'Not Found' in error:
            raise OperationalError('Connection failed')
        else:
            if full_response:
                raise DatabaseError(full_response)
            else:
                raise DatabaseError(error)

    # handle other errors, raised for instance by the http layer
    else:
        raise DatabaseError(str(msg_or_exception))


# Exceptions as defined in PEP-249, the module treats errors using thoses
# classes following as closely as possible the original definitions.


# http://python3porting.com/differences.html#standarderror
try:
    class Warning(Exception):
        pass

    class Error(Exception):
        pass
except NameError:
    class Warning(Exception):
        pass

    class Error(Exception):
        pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass
