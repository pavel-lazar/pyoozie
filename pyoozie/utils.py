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

from lxml import etree

__author__ = 'pavel'


def properties_to_config(properties):
    """
    Transform a dict of properties to an XML configuration file used by oozie

    :param properties: a dict of properties
    :type properties: dict
    :rtype : basestring
    :return: AN XML configuration
    """
    root = etree.Element('configuration')
    for pname, pvalue in properties.iteritems():
        property_element = etree.Element('property')
        property_name = etree.Element('name')
        property_value = etree.Element('value')
        property_name.text = pname
        property_value.text = unicode(pvalue)
        property_element.append(property_name)
        property_element.append(property_value)
        root.append(property_element)

    return etree.tostring(root, encoding='UTF-8', xml_declaration=True, pretty_print=True)
