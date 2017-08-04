#
# Just some general unicode encoding utilities
#

# BeautifulSoup dependency
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
try:
    import BeautifulSoup
except:
    from bs4 import BeautifulSoup, UnicodeDammit


def convertToUnicodeStr(unknown_text):
    # Check to see if it's already unicode
    if type(unknown_text) == unicode:
        return unknown_text

    # Odd but going to support it
    if type(unknown_text) != str:
        try:
            unknown_text = str(unknown_text)
        except UnicodeEncodeError:
            print "Could not convert to str:", type(unknown_text), unknown_text
            raise

    # Empty text happens a lot
    if len(unknown_text) == 0:
        return u''

    # Use BeautifulSoup to try to encode it
    unicode_text = UnicodeDammit(unknown_text).unicode_markup
    if not unicode_text:
        print "Warning: Unicode encoding error, punting..."
        print "Warning: Unknown text:" + unknown_text
        return unicode("Unicode_encoding_error")

    # Assuming success
    return unicode_text


def convertToUnicode(unknown):
    if type(unknown) == list:
        unicode_val = [convertToUnicode(i) for i in unknown]
    elif type(unknown) == dict:
        unicode_val = {}
        for i in unknown:
            unicode_val[i] = convertToUnicode(unknown[i])
    else:
        unicode_val = convertToUnicodeStr(unknown)

    return unicode_val


def convertToUTF8(unknown):
    if type(unknown) == list:
        unicode_val = [convertToUTF8(i) for i in unknown]
    elif type(unknown) == dict:
        unicode_val = {}
        for i in unknown:
            unicode_val[i] = convertToUTF8(unknown[i])
    else:
        unicode_val = convertToUTF8Str(unknown)

    return unicode_val


def convertToUTF8Str(unknown_text):
    unicode_text = convertToUnicode(unknown_text)
    utf8_text = unicode_text.encode("utf-8", "ignore")
    return utf8_text


def convertToAscii(unknown_text):
    unicode_text = convertToUnicode(unknown_text)
    ascii_text = unicode_text.encode("ascii", "ignore")
    return ascii_text
