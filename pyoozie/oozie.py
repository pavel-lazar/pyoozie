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

import httplib
import os
import re

import requests

import errors
import utils

__author__ = 'pavel'
__all__ = ['Oozie']


class SystemStatus:
    NORMAL = 'NORMAL'
    NOWEBSERVICE = 'NOWEBSERVICE'
    SAFEMODE = 'SAFEMODE'


class AdminEndPoint:
    TIME_ZONES = 'admin/available-timezones'
    VERSION = 'admin/build-version'
    INSTRUMENTATION = 'admin/instrumentation'
    CONFIGURATION = 'admin/configuration'
    SYSTEM_STATUS = 'admin/status'
    OS_ENV = 'admin/os-env'
    JAVA_SYS_PROPERTIES = 'admin/java-sys-properties'


class JobAction:
    START = 'start'
    SUSPEND = 'suspend'
    RESUME = 'resume'
    KILL = 'kill'
    DRYRUN = 'dryrun'
    RERUN = 'rerun'
    CHANGE = 'change'


JobsEndPoint = 'jobs'
JobEndPoint = 'job'


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


class Oozie(object):
    """
    Oozie is a python warper for the oozie REST api
    """
    DEFAULT_NAME_NODE = r'hdfs://localhost:8020'
    DEFAULT_JOB_TRACKER = r'localhost:8021'
    DEFAULT_OOZIE_LIBPATH = r'/user/oozie/share/lib/pig'
    DEFAULT_USER_NAME = 'hdfs'
    _EXCEPTION_PATTERN = re.compile(r"<b>exception</b> <pre>(.*)")
    _DESCRIPTION_PATTERN = re.compile(r"<b>description</b> <u>(.*)</u>")

    def __init__(self, hostname='localhost', port=11000):
        """
        Create a new client for interacting with Oozie

        :param hostname: the ip address or hostname of the oozie WS
        :type hostname: basestring
        :param port: the port of the oozie WS
        :type port: int
        """
        self.hostname = hostname
        self.port = port
        self.base_uri = "http://{host}:{port}/oozie/v1/".format(host=self.hostname, port=self.port)

    def create_job(self, config):
        # TODO: validate the config xml file
        """
        Create a standard job based on XML configuration file
        The type of job is determined by the presence of one of the following 3 properties:
            oozie.wf.application.path : path to a workflow application directory, creates a workflow job
            oozie.coord.application.path : path to a coordinator application file, creates a coordinator job
            oozie.bundle.application.path : path to a bundle application file, creates a bundle job
        Or, if none of those are present, the jobtype parameter determines the type of job to run.
        It can either be mapreduce or pig.

        :type config: basestring
        :param config: XML configuration file.
        <?xml version="1.0" encoding="UTF-8"?>
        <configuration>
            <property>
                <name>user.name</name>
                <value>bansalm</value>
            </property>
            <property>
                <name>oozie.wf.application.path</name>
                <value>hdfs://foo:8020/user/bansalm/myapp/</value>
            </property>
            ...
        </configuration>
        :rtype : basestring
        :return: Id of the created job
        :raise errors.OozieError: if the server does not response with a CREATED response
        """
        headers = {'Content-Type': 'application/xml;charset=UTF-8'}
        response = requests.post(self.base_uri + JobsEndPoint, headers=headers, data=config)
        if response.status_code != httplib.CREATED:
            error_description = self._get_error_description_from_response_content(response.content)
            error_exception = self._get_error_exception_from_response_content(response.content)
            raise errors.OozieError(_format_error(error_description, error_exception))
        else:
            return response.json()['id']

    def create_hive_job(self, script, params=None, options=None, files=None, archives=None,
                        user_name=DEFAULT_USER_NAME, name_node=DEFAULT_NAME_NODE, job_tracker=DEFAULT_JOB_TRACKER,
                        oozie_libpath=DEFAULT_NAME_NODE + DEFAULT_OOZIE_LIBPATH):
        """
        Submit a Workflow that contains a single HIVE action without writing a workflow.xml.
        Any requred Jars or other files must already exist in HDFS.

        :param script: Contains the HIVE script you want to run (the actual script, not a file path)
        :type script: basestring
        :param params: A dict of parameters (variable definition for the script) in 'key=value' format
        :type params: dict
        :param options: A list of arguments to pass to HIVE, arguments are sent directly to HIVE without any
                        modification unless they start with -D,
                        in which case they are put into the element of the action
        :type options: list[basestring]
        :param files: A list of files needed for the script (hdfs location)
        :type files: list[basestring]
        :param archives: A list of archives needed for the script (hdfs location)
        :type archives: list[basestring]
        :param user_name: The username of the user submitting the job
        :type user_name: basestring
        :param name_node: The NameNode (e.g: hdfs://localhost:8020
        :type name_node: basestring
        :param job_tracker: The JobTracker (e.g: localhost:8021)
        :type job_tracker: basestring
        :param oozie_libpath: A directory in HDFS that contains necessary Jars for your job (e.g: oozie share lib)
        :type oozie_libpath: basestring
        :return: ID of the created workflow
        :rtype : basestring
        """
        # TODO: remove code duplication with pig job creation
        properties = {'fs.default.name': name_node,
                      'mapred.job.tracker': job_tracker,
                      'user.name': user_name,
                      'oozie.hive.script': script,
                      'oozie.libpath': oozie_libpath,
                      'oozie.proxysubmission': 'true', }
        if files:
            properties['oozie.files'] = ','.join("%s#%s" % (f, os.path.basename(f)) for f in files)

        if archives:
            properties['oozie.archives'] = ','.join("%s#%s" % (f, os.path.basename(f)) for f in files)

        if params:
            properties['oozie.hive.script.params.size'] = len(params)
            for i, param in enumerate(params.iteritems()):
                properties['oozie.hive.script.params.%d' % i] = "%s=%s" % param

        if options:
            properties['oozie.hive.options.size'] = len(options)
            for i, option in enumerate(options):
                properties['oozie.hive.options.%d' % i] = option

        config = utils.properties_to_config(properties)
        headers = {'Content-Type': 'application/xml;charset=UTF-8'}
        response = requests.post(self.base_uri + JobsEndPoint, params={'jobtype': 'hive'}, headers=headers, data=config)
        if response.status_code != httplib.CREATED:
            error_description = self._get_error_description_from_response_content(response.content)
            error_exception = self._get_error_exception_from_response_content(response.content)
            raise errors.OozieError(_format_error(error_description, error_exception))
        else:
            return response.json()['id']

    def create_pig_job(self, script, params=None, options=None, files=None, archives=None,
                       user_name=DEFAULT_USER_NAME, name_node=DEFAULT_NAME_NODE, job_tracker=DEFAULT_JOB_TRACKER,
                       oozie_libpath=DEFAULT_NAME_NODE + DEFAULT_OOZIE_LIBPATH):
        """
        Submit a Workflow that contains a single PIG action without writing a workflow.xml.
        Any requred Jars or other files must already exist in HDFS.

        :param script: Contains the PIG script you want to run (the actual script, not a file path)
        :type script: basestring
        :param params: A dict of parameters (variable definition for the script) in 'key=value' format
        :type params: dict
        :param options: A list of arguments to pass to PIG, arguments are sent directly to Pig without any modification
                        unless they start with -D , in which case they are put into the element of the action
        :type options: list[basestring]
        :param files: A list of files needed for the script (hdfs location)
        :type files: list[basestring]
        :param archives: A list of archives needed for the script (hdfs location)
        :type archives: list[basestring]
        :param user_name: The username of the user submitting the job
        :type user_name: basestring
        :param name_node: The NameNode (e.g: hdfs://localhost:8020
        :type name_node: basestring
        :param job_tracker: The JobTracker (e.g: localhost:8021)
        :type job_tracker: basestring
        :param oozie_libpath: A directory in HDFS that contains necessary Jars for your job (e.g: oozie share lib)
        :type oozie_libpath: basestring
        :return: ID of the created workflow
        :rtype : basestring
        """
        properties = {'fs.default.name': name_node,
                      'mapred.job.tracker': job_tracker,
                      'user.name': user_name,
                      'oozie.pig.script': script,
                      'oozie.libpath': oozie_libpath,
                      'oozie.proxysubmission': 'true', }
        if files:
            properties['oozie.files'] = ','.join("%s#%s" % (f, os.path.basename(f)) for f in files)

        if archives:
            properties['oozie.archives'] = ','.join("%s#%s" % (f, os.path.basename(f)) for f in files)

        if params:
            properties['oozie.pig.script.params.size'] = len(params)
            for i, param in enumerate(params.iteritems()):
                properties['oozie.pig.script.params.%d' % i] = "%s=%s" % param

        if options:
            properties['oozie.pig.options.size'] = len(options)
            for i, option in enumerate(options):
                properties['oozie.pig.options.%d' % i] = option

        config = utils.properties_to_config(properties)
        headers = {'Content-Type': 'application/xml;charset=UTF-8'}
        response = requests.post(self.base_uri + JobsEndPoint, params={'jobtype': 'pig'}, headers=headers, data=config)
        if response.status_code != httplib.CREATED:
            error_description = self._get_error_description_from_response_content(response.content)
            error_exception = self._get_error_exception_from_response_content(response.content)
            raise errors.OozieError(_format_error(error_description, error_exception))
        else:
            return response.json()['id']

    def do_job_action(self, job_id, action, config=None):
        """
        starts, suspends, resumes, kills, or dryruns a job.
        Rerunning and changing a job require additional parameters,
        :param job_id: The workflow to act on
        :type job_id: basestring
        :param action: The action to do ('start', 'suspend', 'resume', 'kill', 'dryrun', 'rerun', and 'change')
        :type action: str
        :param config: if rerunning or changing supply with the XML configuration
        :type config: basestring
        :raise errors.OozieError: if the server does not response with an OK response
        """
        if action not in [v for v in dir(JobAction) if not v.startswith('_')]:
            raise ValueError('%s is not a legal action' % action)
        if config is not None:
            headers = {'Content-Type': 'application/xml;charset=UTF-8'}
            response = requests.put(self.base_uri + JobEndPoint + "/" + job_id,
                                    params={'action': action}, headers=headers, data=config)
        else:
            response = requests.put(self.base_uri + JobEndPoint + "/" + job_id, params={'action': action})

        if response.status_code != httplib.OK:
            error_description = self._get_error_description_from_response_content(response.content)
            error_exception = self._get_error_exception_from_response_content(response.content)
            raise errors.OozieError(_format_error(error_description, error_exception))

    def get_job_information(self, job_id, timezone='GMT'):
        """
        Retrieves the job information.

        :param job_id: The JOB ID
        :type job_id: basestring
        :param timezone: The timezone to use for times
        :type timezone: basestring
        :return: The information of the job
        :rtype : dict
        """
        response = requests.get(self.base_uri + JobEndPoint + "/" + job_id,
                                params={'show': 'info', 'timezone': timezone})
        if response.status_code == httplib.OK:
            return response.json()
        elif response.status_code == httplib.BAD_REQUEST:
            raise ValueError('%s is a bad job id' % job_id)
        else:
            error_description = self._get_error_description_from_response_content(response.content)
            error_exception = self._get_error_exception_from_response_content(response.content)
            raise errors.OozieError(_format_error(error_description, error_exception))

    def get_job_definition(self, job_id):
        """
        Retrieves the workflow or a coordinator job definition file.

        :param job_id: The JOB ID
        :type job_id: basestring
        :return: The XML definition file
        :rtype : basestring
        """
        response = requests.get(self.base_uri + JobEndPoint + "/" + job_id,
                                params={'show': 'definition'})
        if response.status_code == httplib.OK:
            return response.content
        elif response.status_code == httplib.BAD_REQUEST:
            raise ValueError('%s is a bad job id' % job_id)
        else:
            error_description = self._get_error_description_from_response_content(response.content)
            error_exception = self._get_error_exception_from_response_content(response.content)
            raise errors.OozieError(_format_error(error_description, error_exception))

    def get_job_log(self, job_id):
        """
        Retrieves the workflow or a coordinator job definition file.

        :param job_id: The JOB ID
        :type job_id: basestring
        :return: The job log
        :rtype : basestring
        """
        response = requests.get(self.base_uri + JobEndPoint + "/" + job_id,
                                params={'show': 'log'})
        if response.status_code == httplib.OK:
            return response.content
        elif response.status_code == httplib.BAD_REQUEST:
            raise ValueError('%s is a bad job id' % job_id)
        else:
            error_description = self._get_error_description_from_response_content(response.content)
            error_exception = self._get_error_exception_from_response_content(response.content)
            raise errors.OozieError(_format_error(error_description, error_exception))

    def get_all_jobs_information(self, timezone='GMT'):
        """
        Retrieves workflow and coordinator jobs information

        :param timezone: The timezone to use for times
        :type timezone: basestring
        :return: A list of all jobs information
        :rtype : list[dict]
        """
        response = requests.get(self.base_uri + JobsEndPoint, params={'timezone': timezone})
        if response.status_code == httplib.OK:
            return response.json['jobs']
        else:
            error_description = self._get_error_description_from_response_content(response.content)
            error_exception = self._get_error_exception_from_response_content(response.content)
            raise errors.OozieError(_format_error(error_description, error_exception))

    def _get_system_status(self):
        response = requests.get(self.base_uri + AdminEndPoint.SYSTEM_STATUS)
        return response.json()['systemMode']

    def _set_system_status(self, status):
        if status not in (SystemStatus.NORMAL, SystemStatus.NOWEBSERVICE, SystemStatus.SAFEMODE):
            raise ValueError('%s is not a legall status' % status)
        response = requests.put(self.base_uri + AdminEndPoint.SYSTEM_STATUS, params={'systemmode': status})
        if response.status_code != httplib.OK:
            error_description = self._get_error_description_from_response_content(response.content)
            error_exception = self._get_error_exception_from_response_content(response.content)
            raise errors.OozieError(_format_error(error_description, error_exception))

    @property
    def os_env(self):
        """
        Oozie system OS environment
        :rtype : dict
        """
        return requests.get(self.base_uri + AdminEndPoint.OS_ENV).json()

    @property
    def java_system_properties(self):
        """
        Oozie Java system properties.
        :rtype : dict
        """
        return requests.get(self.base_uri + AdminEndPoint.JAVA_SYS_PROPERTIES).json()

    @property
    def configuration(self):
        """
        Oozie system configuration
        :rtype : dict
        """
        return requests.get(self.base_uri + AdminEndPoint.CONFIGURATION).json()

    @property
    def instrumentation(self):
        """
        Oozie instrumentation information
        :rtype : dict
        """
        return requests.get(self.base_uri + AdminEndPoint.INSTRUMENTATION).json()

    @property
    def version(self):
        """
        Oozie build version
        :rtype : basestring
        """
        return requests.get(self.base_uri + AdminEndPoint.VERSION).json()['buildVersion']

    @property
    def time_zones(self):
        """
        available time zones
        :rtype : list[dict]
        """
        return requests.get(self.base_uri + AdminEndPoint.TIME_ZONES).json()['available-timezones']

    def _get_error_description_from_response_content(self, content):
        """
        Parses the oozie server response on error to get the description of the error

        :type content: basestring
        :param content: html page of repsonse (taken from repsonse.content)
        :return: error description string 
        :rtype : basestring
        """
        return self._DESCRIPTION_PATTERN.findall(content)[0]

    def _get_error_exception_from_response_content(self, content):
        """
        Parses the oozie server response on error to get the exception
        :type content: basestring
        :param content: html page of repsonse (taken from repsonse.content)
        :rtype : basestring
        :return: error exception
        """
        return self._EXCEPTION_PATTERN.findall(content)[0]

    system_status = property(_get_system_status, _set_system_status, None,
                             "Oozie system status. NORMAL, NOWEBSERVICE, or SAFEMODE")