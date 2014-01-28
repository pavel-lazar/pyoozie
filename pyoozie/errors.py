#!/usr/bin/env python
# Licensed to Pavel Lazar,  under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Pavel Lazar licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import re

__author__ = 'pavel'

_EXCEPTION_PATTERN = re.compile(r"<b>exception</b> <pre>(.*)")
_DESCRIPTION_PATTERN = re.compile(r"<b>description</b> <u>(.*)</u>")


class OozieError(Exception):
    """Base exception for library exception"""
    pass


def _get_error_description_from_response_content(content):
    """
    Parses the oozie server response on error to get the description of the error

    :type content: basestring
    :param content: html page of repsonse (taken from repsonse.content)
    :return: error description string
    :rtype : basestring
    """
    return _DESCRIPTION_PATTERN.findall(content)[0]


def _get_error_exception_from_response_content(content):
    """
    Parses the oozie server response on error to get the exception
    :type content: basestring
    :param content: html page of repsonse (taken from repsonse.content)
    :rtype : basestring
    :return: error exception
    """
    try:
        return _EXCEPTION_PATTERN.findall(content)[0]
    except IndexError:
        return None


def _format_error(description, exception):
    """
    format an error message from responses taken from the server
    :type description: basestring
    :param description: The error description taken from the server's response
    :type exception: basestring
    :param exception: The exception string taken from the server's response
    :rtype : basestring
    """
    if exception:
        return "%s: %s" % (description, exception)
    else:
        return description


def error_message_from_response(response):
    """
    Parse oozie's error message from response
    :param response: Server's response
    :rtype : str
    :return: Formatted error message
    """
    error_message = response.headers.get('oozie-error-message', None)
    if not error_message:
        error_description = _get_error_description_from_response_content(response.content)
        error_exception = _get_error_exception_from_response_content(response.content)
        error_message = _format_error(error_description, error_exception)

    return error_message