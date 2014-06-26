# !/usr/bin/env python
# Licensed to Pavel Lazar,  under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Pavel Lazar licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os

from lxml import etree

__author__ = 'pavel'


class Node(object):
    def __init__(self, name):
        """
        You should never create an instance of an abstract node
        """
        self.name = name

    def to_xml(self):
        """
        Serialize the node to XML element tree
        :rtype : etree.Element
        """
        raise NotImplementedError

    def to_string(self, encoding='UTF-8', pretty_print=True, xml_declaration=False):
        """
        Get string representation of the node (an XML string)
        :param encoding: output encoding
        :param pretty_print: Pretty format the XML?
        :param xml_declaration: To add XML declaration?
        :return: basestring
        """
        return etree.tostring(self.to_xml(), encoding=encoding, xml_declaration=xml_declaration,
                              pretty_print=pretty_print)

    def __str__(self):
        return self.to_string()

    def get_child_nodes(self):
        """
        Get all child nodes of the current node
        :rtype : list[Node]
        :return: all sub nodes of the current node
        """
        return []


class ControlFlowNode(Node):
    """
    Abstract control flow node.
    Control flow nodes define the beginning and the end of a workflow (the start , end and kill nodes)
    and provide a mechanism to control the workflow execution path (the decision , fork and join nodes).
    """

    def __init__(self, name):
        super(ControlFlowNode, self).__init__(name)


class ActionNode(Node):
    """
    Abstract action node.
    Action nodes are the mechanism by which a workflow triggers the execution of a computation/processing task.
    """

    def __init__(self, name, ok, error):
        """
            Construct an action node.

            :param name: name of the action
            :param ok: name to transition when successful
            :param error: name to transition when action fails to complete
            """
        super(ActionNode, self).__init__(name)
        self.ok = ok
        self.error = error

    def to_xml(self):
        """
        Serialize the node to XML element tree
        :rtype : etree.Element
        """
        ok = self.ok.name if isinstance(self.ok, Node) else self.ok
        error = self.error.name if isinstance(self.error, Node) else self.error
        action_root = etree.Element('action', name=self.name)
        etree.SubElement(action_root, 'ok', to=ok)
        etree.SubElement(action_root, 'error', to=error)
        return action_root

    def get_child_nodes(self):
        """
        Get all child nodes of the current node
        :rtype : list[Node]
        :return: all sub nodes of the current node
        """
        nodes = []
        if isinstance(self.ok, Node):
            nodes.append(self.ok)
        if isinstance(self.error, Node):
            nodes.append(self.error)

        return nodes


class StartNode(ControlFlowNode):
    def __init__(self, name):
        """
        The start node is the entry point for a workflow job,
        it indicates the first workflow node the workflow job must transition to.

        :param name: name of first workflow node to execute.
        """
        super(StartNode, self).__init__(name)

    def to_xml(self):
        """
        Serialize the node to XML element tree
        :rtype : etree.Element
        """
        to = self.name.name if isinstance(self.name, Node) else self.name
        return etree.Element('start', to=to)

    def get_child_nodes(self):
        """
        Get all child nodes of the current node
        :rtype : list[Node]
        :return: all sub nodes of the current node
        """
        nodes = []
        if isinstance(self.name, Node):
            nodes.append(self.name)

        return nodes


class EndNode(ControlFlowNode):
    def __init__(self, name):
        """
        The end node is the end for a workflow job, it indicates that the workflow job has completed successfully.

        When a workflow job reaches the end it finishes successfully (SUCCEEDED).
        If one or more actions started by the workflow job are executing when the end node is reached,
        the actions will be killed. In this scenario the workflow job is still considered as successfully run.

        :param name: name of the transition to do to end the workflow job.
        """
        super(EndNode, self).__init__(name)

    def to_xml(self):
        """
        Serialize the node to XML element tree
        :rtype : etree.Element
        """
        return etree.Element('end', name=self.name)


class KillNode(ControlFlowNode):
    DEFAULT_KILL_MESSAGE = 'Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]'

    def __init__(self, name, message=DEFAULT_KILL_MESSAGE):
        """
        The kill node allows a workflow job to kill itself.

        When a workflow job reaches the kill it finishes in error (KILLED).
        If one or more actions started by the workflow job are executing when the kill node is reached,
        the actions will be killed.

        :param name: name of the Kill action node.
        :param message: data to be logged as the kill reason for the workflow job.
        """
        super(KillNode, self).__init__(name)
        self.message = message

    def to_xml(self):
        """
        Serialize the node to XML element tree
        :rtype : etree.Element
        """
        root = etree.Element('kill', name=self.name)
        etree.SubElement(root, 'message').text = self.message
        return root


class DecisionNode(ControlFlowNode):
    def __init__(self, name, cases, default):
        """
        A decision node enables a workflow to make a selection on the execution path to follow.

        The behavior of a decision node can be seen as a switch-case statement.
        A decision node consists of a list of predicates-transition pairs plus a default transition.
        Predicates are evaluated in order or appearance until one of them evaluates to true and the corresponding
        transition is taken. If none of the predicates evaluates to true the default transition is taken.



        :param name: name of the decision node.
        :param cases: A list of cases, each contains a predicate and a transition name. [(name, predicate)]
        :type cases: list(tuple(str, str))
        :param default:  the transition to take if none of the predicates evaluates to true.
        """
        super(DecisionNode, self).__init__(name)
        self.cases = cases
        self.default = default

    def to_xml(self):
        """
        Serialize the node to XML element tree
        :rtype : etree.Element
        """
        decision = etree.Element('decision', name=self.name)
        switch = etree.SubElement(decision, 'switch')
        for case_to, case_predicate in self.cases:
            to = case_to.name if isinstance(case_to, Node) else case_to
            etree.SubElement(switch, 'case', to=to).text = case_predicate
        default_to = self.default.name if isinstance(self.default, Node) else self.default
        etree.SubElement(switch, 'default', to=default_to)
        return decision

    def get_child_nodes(self):
        """
        Get all child nodes of the current node
        :rtype : list[Node]
        :return: all sub nodes of the current node
        """
        nodes = []
        for case_to, _ in self.cases:
            if isinstance(case_to, Node):
                nodes.append(case_to)

        if isinstance(self.default, Node):
            nodes.append(self.default)

        return nodes


class ForkNode(ControlFlowNode):
    def __init__(self, name, paths):
        """
        A fork node splits one path of execution into multiple concurrent paths of execution.

        :param name: name of the workflow fork node.
        :param paths: names of the workflow nodes that will be part of the concurrent execution paths.
        :type paths: list[str or NodeAction]
        """
        super(ForkNode, self).__init__(name)
        self.paths = paths

    def to_xml(self):
        """
        Serialize the node to XML element tree
        :rtype : etree.Element
        """
        fork = etree.Element('fork', name=self.name)
        for path in self.paths:
            start = path.name if isinstance(path, Node) else path
            etree.SubElement(fork, 'path', start=start)
        return fork

    def get_child_nodes(self):
        """
        Get all child nodes of the current node
        :rtype : list[Node]
        :return: all sub nodes of the current node
        """
        nodes = []
        for path in self.paths:
            if isinstance(path, Node):
                nodes.append(path)

        return nodes


class JoinNode(ControlFlowNode):
    def __init__(self, name, to):
        """
        A join node waits until every concurrent execution path of a previous fork node arrives to it.

        :param name: name of the workflow join node.
        :param to:  name of the workflow node that will executed after all concurrent execution paths of the
                    corresponding fork arrive to the join node.
        """
        super(JoinNode, self).__init__(name)
        self.to = to

    def to_xml(self):
        """
        Serialize the node to XML element tree
        :rtype : etree.Element
        """
        to = self.to.name if isinstance(self.to, Node) else self.to
        return etree.Element('join', name=self.name, to=to)

    def get_child_nodes(self):
        """
        Get all child nodes of the current node
        :rtype : list[Node]
        :return: all sub nodes of the current node
        """
        nodes = []
        if isinstance(self.to, Node):
            nodes.append(self.to)

        return nodes


class PigAction(ActionNode):
    def __init__(self, name, ok, error, script, delete_paths=None, mkdir_paths=None, job_xml=None, properties=None,
                 params=None, arguments=None, files=None, archives=None, name_node='${nameNode}',
                 job_tracker='${jobTracker}'):
        """
        Create a PIG action

        :param name: name of the action
        :param ok: name to transition when successful
        :param error: name to transition when action fails to complete
        :param script: Contains the PIG script you want to run (a file path in hdfs)
        :type script: basestring
        :param delete_paths: a list of paths (in hdfs) to delete before starting the pig job
        :type delete_paths: list[str]
        :param mkdir_paths: a list of dir paths (in hdfs) to delete before starting the pig job
        :type mkdir_paths: list[str]
        :param job_xml:  if present, must refer to a Hadoop JobConf job.xml file bundled in the workflow application.
        :type job_xml: str
        :param properties: a dict of hadoop configuration properties in key=value format
        :type properties: dict
        :param params: A dict of parameters (variable definition for the script) in 'key=value' format
        :type params: dict
        :param arguments: A list of arguments to pass to PIG
        :type arguments: list[basestring]
        :param files: A list of files needed for the script (hdfs location)
        :type files: list[basestring]
        :param archives: A list of archives needed for the script (hdfs location)
        :type archives: list[basestring]
        :param name_node: The NameNode (e.g: hdfs://localhost:8020
        :type name_node: basestring
        :param job_tracker: The JobTracker (e.g: localhost:8021)
        :type job_tracker: basestring
        """
        super(PigAction, self).__init__(name, ok, error)
        self.script = script
        self.delete_paths = delete_paths or []
        self.mkdir_paths = mkdir_paths or []
        self.job_xml = job_xml
        self.properties = properties or {}
        self.params = params or {}
        self.arguments = arguments or []
        self.files = files or []
        self.archives = archives or []
        self.name_node = name_node
        self.job_tracker = job_tracker

    def to_xml(self):
        """
        Serialize the node to XML element tree
        :rtype : etree.Element
        """
        action = super(PigAction, self).to_xml()
        pig = etree.SubElement(action, 'pig')

        if self.job_tracker:
            etree.SubElement(pig, 'job-tracker').text = self.job_tracker

        if self.name_node:
            etree.SubElement(pig, 'name-node').text = self.name_node

        if self.delete_paths or self.mkdir_paths:
            prepare = etree.SubElement(pig, 'prepare')
            for delete_path in self.delete_paths:
                etree.SubElement(prepare, 'delete', path=delete_path)

            for mkdir_path in self.mkdir_paths:
                etree.SubElement(prepare, 'mkdir', path=mkdir_path)

        if self.job_xml:
            etree.SubElement(pig, 'job-xml').text = self.job_xml

        if self.properties:
            configuration = etree.SubElement(pig, 'configuration')
            for name, value in self.properties.iteritems():
                config_property = etree.SubElement(configuration, 'property')
                etree.SubElement(config_property, 'name').text = name
                etree.SubElement(config_property, 'value').text = value

        etree.SubElement(pig, 'script').text = self.script

        for param in self.params.iteritems():
            etree.SubElement(pig, 'param').text = '%s=%s' % param

        for argument in self.arguments:
            etree.SubElement(pig, 'argument').text = argument

        for file_path in self.files:
            etree.SubElement(pig, 'file').text = "%s#%s" % (file_path, os.path.basename(file_path))

        for archive in self.archives:
            etree.SubElement(pig, 'archive').text = "%s#%s" % (archive, os.path.basename(archive))

        return action


class HiveAction(ActionNode):
    def __init__(self, name, ok, error, script, delete_paths=None, mkdir_paths=None, job_xml=None, properties=None,
                 params=None, files=None, archives=None, name_node='${nameNode}', job_tracker='${jobTracker}'):
        """
        Starts a HIVE job

        :param name: name of the action
        :param ok: name to transition when successful
        :param error: name to transition when action fails to complete
        :param script: Contains the PIG script you want to run (a file path in hdfs)
        :type script: basestring
        :param delete_paths: a list of paths (in hdfs) to delete before starting the hive job
        :type delete_paths: list[str]
        :param mkdir_paths: a list of dir paths (in hdfs) to delete before starting the hive job
        :type mkdir_paths: list[str]
        :param job_xml:  if present, must refer to a Hadoop JobConf job.xml file bundled in the workflow application.
        :type job_xml: str
        :param properties: a dict of hadoop configuration properties in key=value format
        :type properties: dict
        :param params: A dict of parameters (variable definition for the script) in 'key=value' format
        :type params: dict
        :param files: A list of files needed for the script (hdfs location)
        :type files: list[basestring]
        :param archives: A list of archives needed for the script (hdfs location)
        :type archives: list[basestring]
        :param name_node: The NameNode (e.g: hdfs://localhost:8020
        :type name_node: basestring
        :param job_tracker: The JobTracker (e.g: localhost:8021)
        :type job_tracker: basestring
        """
        super(HiveAction, self).__init__(name, ok, error)
        self.script = script
        self.delete_paths = delete_paths or []
        self.mkdir_paths = mkdir_paths or []
        self.job_xml = job_xml
        self.properties = properties or {}
        self.params = params or {}
        self.files = files or []
        self.archives = archives or []
        self.name_node = name_node
        self.job_tracker = job_tracker

    def to_xml(self):
        """
        Serialize the node to XML element tree
        :rtype : etree.Element
        """
        action = super(HiveAction, self).to_xml()
        hive = etree.SubElement(action, 'hive', xmlns='uri:oozie:hive-action:0.2')

        if self.job_tracker:
            etree.SubElement(hive, 'job-tracker').text = self.job_tracker

        if self.name_node:
            etree.SubElement(hive, 'name-node').text = self.name_node

        if self.delete_paths or self.mkdir_paths:
            prepare = etree.SubElement(hive, 'prepare')
            for delete_path in self.delete_paths:
                etree.SubElement(prepare, 'delete', path=delete_path)

            for mkdir_path in self.mkdir_paths:
                etree.SubElement(prepare, 'mkdir', path=mkdir_path)

        if self.job_xml:
            etree.SubElement(hive, 'job-xml').text = self.job_xml

        if self.properties:
            configuration = etree.SubElement(hive, 'configuration')
            for name, value in self.properties.iteritems():
                property = etree.SubElement(configuration, 'property')
                etree.SubElement(property, 'name').text = name
                etree.SubElement(property, 'value').text = value

        etree.SubElement(hive, 'script').text = self.script

        for param in self.params.iteritems():
            etree.SubElement(hive, 'param').text = '%s=%s' % param

        for file_path in self.files:
            etree.SubElement(hive, 'file').text = "%s#%s" % (file_path, os.path.basename(file_path))

        for archive in self.archives:
            etree.SubElement(hive, 'archive').text = "%s#%s" % (archive, os.path.basename(archive))

        return action


class FsAction(ActionNode):
    def __init__(self, name, ok, error, delete_paths=None, mkdir_paths=None, moves=None, properties=None, job_xml=None,
                 name_node='${nameNode}'):
        """
        Create a FS action
        :param name: name of the action
        :param ok: name to transition when successful
        :param error: name to transition when action fails to complete
        :param delete_paths: a list of paths (in hdfs) to delete before starting the hive job
        :type delete_paths: list[str]
        :param mkdir_paths: a list of dir paths (in hdfs) to delete before starting the hive job
        :type mkdir_paths: list[str]
        :param moves: a list of moves from src to dst. where each move is (src, dst)
        :type moves: list[tuple]
        :param properties: a dict of hadoop configuration properties in key=value format
        :type properties: dict
        :param job_xml:  if present, must refer to a Hadoop JobConf job.xml file bundled in the workflow application.
        :type job_xml: str
        :param name_node: The NameNode (e.g: hdfs://localhost:8020
        :type name_node: basestring
        """
        super(FsAction, self).__init__(name, ok, error)
        self.delete_paths = delete_paths or []
        self.mkdir_paths = mkdir_paths or []
        self.moves = moves or []
        self.properties = properties or {}
        self.job_xml = job_xml
        self.name_node = name_node

    def to_xml(self):
        """
        Serialize the node to XML element tree
        :rtype : etree.Element
        """
        action = super(FsAction, self).to_xml()
        fs = etree.SubElement(action, 'FS')

        if self.name_node:
            etree.SubElement(fs, 'name-node').text = self.name_node

        for delete_path in self.delete_paths:
            etree.SubElement(fs, 'delete', path=delete_path)

        for mkdir_path in self.mkdir_paths:
            etree.SubElement(fs, 'mkdir', path=mkdir_path)

        for src, dst in self.moves:
            etree.SubElement(fs, 'move', source=src, target=dst)

        if self.job_xml:
            etree.SubElement(fs, 'job-xml').text = self.job_xml

        if self.properties:
            configuration = etree.SubElement(fs, 'configuration')
            for name, value in self.properties.iteritems():
                property = etree.SubElement(configuration, 'property')
                etree.SubElement(property, 'name').text = name
                etree.SubElement(property, 'value').text = value

        return action


class ShellAction(ActionNode):
    def __init__(self, name, ok, error, command, delete_paths=None, mkdir_paths=None, job_xml=None, properties=None,
                 arguments=None, env_vars=None, files=None, archives=None, capture_output=False,
                 name_node='${nameNode}',
                 job_tracker='${jobTracker}'):
        """
        Create a Shell action

        :param name: name of the action
        :param ok: name to transition when successful
        :param error: name to transition when action fails to complete
        :param command: Contains the shell command you want to run (a file path in hdfs)
        :type command: basestring
        :param delete_paths: a list of paths (in hdfs) to delete before starting the pig job
        :type delete_paths: list[str]
        :param mkdir_paths: a list of dir paths (in hdfs) to delete before starting the pig job
        :type mkdir_paths: list[str]
        :param job_xml:  if present, must refer to a Hadoop JobConf job.xml file bundled in the workflow application.
        :type job_xml: str
        :param properties: a dict of hadoop configuration properties in key=value format
        :type properties: dict
        :param arguments: A list of arguments to pass to the shell.
        :type arguments: list[basestring]
        :param env_vars: A dict of environment variable in a key=value form
        :type env_vars: dict
        :param files: A list of files needed for the script (hdfs location)
        :type files: list[basestring]
        :param archives: A list of archives needed for the script (hdfs location)
        :type archives: list[basestring]
        :param capture_output: A flag indicating if to capture output or not
        :type capture_output: bool
        :param name_node: The NameNode (e.g: hdfs://localhost:8020
        :type name_node: basestring
        :param job_tracker: The JobTracker (e.g: localhost:8021)
        :type job_tracker: basestring
        """
        super(ShellAction, self).__init__(name, ok, error)
        self.command = command
        self.delete_paths = delete_paths or []
        self.mkdir_paths = mkdir_paths or []
        self.job_xml = job_xml
        self.properties = properties or {}
        self.arguments = arguments or []
        self.env_vars = env_vars or {}
        self.files = files or []
        self.archives = archives or []
        self.capture_output = capture_output
        self.name_node = name_node
        self.job_tracker = job_tracker

    def to_xml(self):
        """
        Serialize the node to XML element tree
        :rtype : etree.Element
        """
        action = super(ShellAction, self).to_xml()
        shell = etree.SubElement(action, 'shell', xmlns="uri:oozie:shell-action:0.1")

        if self.job_tracker:
            etree.SubElement(shell, 'job-tracker').text = self.job_tracker

        if self.name_node:
            etree.SubElement(shell, 'name-node').text = self.name_node

        if self.delete_paths or self.mkdir_paths:
            prepare = etree.SubElement(shell, 'prepare')
            for delete_path in self.delete_paths:
                etree.SubElement(prepare, 'delete', path=delete_path)

            for mkdir_path in self.mkdir_paths:
                etree.SubElement(prepare, 'mkdir', path=mkdir_path)

        if self.job_xml:
            etree.SubElement(shell, 'job-xml').text = self.job_xml

        if self.properties:
            configuration = etree.SubElement(shell, 'configuration')
            for name, value in self.properties.iteritems():
                property = etree.SubElement(configuration, 'property')
                etree.SubElement(property, 'name').text = name
                etree.SubElement(property, 'value').text = value

        etree.SubElement(shell, 'exec').text = self.command

        for var in self.env_vars.iteritems():
            etree.SubElement(shell, 'env-var').text = '%s=%s' % var

        for argument in self.arguments:
            etree.SubElement(shell, 'argument').text = argument

        for file_path in self.files:
            etree.SubElement(shell, 'file').text = "%s#%s" % (file_path, os.path.basename(file_path))

        for archive in self.archives:
            etree.SubElement(shell, 'archive').text = "%s#%s" % (archive, os.path.basename(archive))

        if self.capture_output:
            etree.SubElement(shell, 'capture-output')

        return action


class EmailAction(ActionNode):
    def __init__(self, name, ok, error, to, subject, body, cc=None):
        """
        Create an Email action

        :param name: name of the action
        :param ok: name to transition when successful
        :param error: name to transition when action fails to complete
        :param to: comma separated list of emails
        :type to: str
        :param subject: An email's subject
        :type subject: basestring
        :param body: An email's body
        :param cc: an optional comma separated list of emails
        :type cc: str
        """
        super(EmailAction, self).__init__(name, ok, error)
        self.to = to
        self.subject = subject
        self.body = body
        self.cc = cc

    def to_xml(self):
        """
        Serialize the node to XML element tree
        :rtype : etree.Element
        """
        action = super(EmailAction, self).to_xml()
        email = etree.SubElement(action, 'email', xmlns="uri:oozie:email-action:0.1")
        etree.SubElement(email, 'to').text = self.to

        if self.cc:
            etree.SubElement(email, 'cc').text = self.cc

        etree.SubElement(email, 'subject').text = self.subject
        etree.SubElement(email, 'body').text = self.body

        return action


class Workflow(object):
    def __init__(self, name, start, parameters=None):
        """
        Create an oozie workflow.
        The workflow is created from a base start node.

        :param name: Name of the workflow
        :param start: A start node
        :param parameters: a dict of key=value parameters to be passed to a workflow. The key is the name and the value
                            is default value of the parameters
        """
        self.nodes = []
        self.name = name
        self.start = start
        self.parameters = parameters or {}

    def to_xml(self):
        """
        Serialize the node to XML element tree
        :rtype : etree.Element
        """
        self._collect_all_nodes()
        workflow = etree.Element('workflow-app', name=self.name, xmlns="uri:oozie:workflow:0.4")
        for node in self.nodes:
            workflow.append(node.to_xml())

        return workflow

    def to_string(self, encoding='UTF-8', pretty_print=True, xml_declaration=False):
        """
        Get string representation of the node (an XML string)
        :param encoding: output encoding
        :param pretty_print: Pretty format the XML?
        :param xml_declaration: To add XML declaration?
        :return: basestring
        """
        return etree.tostring(self.to_xml(), encoding=encoding, xml_declaration=xml_declaration,
                              pretty_print=pretty_print)

    def __str__(self):
        return self.to_string()

    def _collect_all_nodes(self):
        """
        Collect all sub nodes of the current workflow, starting from 'start'
        """
        self.nodes = []
        visited = set()
        nodes_to_visit = [self.start]
        while nodes_to_visit:
            current_node = nodes_to_visit.pop(0)
            self.nodes.append(current_node)
            visited.add(current_node)
            for node in current_node.get_child_nodes():
                if node not in visited and node not in nodes_to_visit:
                    nodes_to_visit.append(node)


if __name__ == "__main__":
    # print etree.tostring(ActionNode('bla', 'ok', 'fail').to_xml(), encoding='UTF-8', xml_declaration=False,
    # pretty_print=True)
    kill = KillNode('kill')
    end = EndNode('end')
    shell = ShellAction('shell1', end, kill, "mycommand.sh", delete_paths=['/a/b', '/a/c'], mkdir_paths=['/d/v'],
                        env_vars={'bla': 'bla_var'}, capture_output=True)

    join = JoinNode('join', shell)

    hive1 = HiveAction('hive_action1', join, end, 'script.hql', delete_paths=['/a/b', '/a/c'], mkdir_paths=['/d/v'],
                       job_xml='/bla/job.xml', params={'input_value': '${input_value}'},
                       files=['/a/file1.py', '/a/file2.py'], archives=['/a/arch.zip'])
    hive2 = HiveAction('hive_action2', join, end, 'script.hql', delete_paths=['/a/b', '/a/c'], mkdir_paths=['/d/v'],
                       job_xml='/bla/job.xml', params={'input_value': '${input_value}'},
                       files=['/a/file1.py', '/a/file2.py'], archives=['/a/arch.zip'])

    pig1 = PigAction('pig_action1', hive1, kill, 'script.pig', delete_paths=['/a/b', '/a/c'], mkdir_paths=['/d/v'],
                     job_xml='/bla/job.xml', params={'input_value': '${input_value}'}, arguments=['bla00'],
                     files=['/a/file1.py', '/a/file2.py'], archives=['/a/arch.zip'])
    pig2 = PigAction('pig_action2', hive2, kill, 'script.pig', delete_paths=['/a/b', '/a/c'], mkdir_paths=['/d/v'],
                     job_xml='/bla/job.xml', params={'input_value': '${input_value}'}, arguments=['bla00'],
                     files=['/a/file1.py', '/a/file2.py'], archives=['/a/arch.zip'])
    fork = ForkNode('fork1', [pig1, pig2])
    start = StartNode(fork)

    workflow = Workflow('test_workflow', start)

    print workflow










