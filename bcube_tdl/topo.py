"""Builder for a BCube Topology

Copyright 2015 Google Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

BCube is a recursive topology, described in the following SIGCOMM paper
http://research.microsoft.com/pubs/81063/comm136-guo.pdf
"""

from condor.tdl import tdl_switching_component
from condor.tdl import tdl_component
from condor.tdl import tdl_endhost
from condor.tdl import tdl_port

from condor.connect.physical import connector as connectorCSP_lib
from condor.connect.physical.constraints import pairwise as pairCSPCstr_lib


class BCubeContainer(tdl_component.TDLComponent):

  def __init__(self, k, n):

    # initialize the underlying ComponentTemplate, store information
    super(BCubeContainer, self).__init__(name="bcube")
    self.top_level_idx = k
    self.num_sub_bcubes_per_bcube = n
    self.num_servers_bcube_0 = n
    self.num_ports_bcube_sw = n

    # initialize port objects used to balance BCube connectivity at each level
    self.server_port_ct_per_level = {}
    for i in xrange(0, self.top_level_idx + 1):
      self.server_port_ct_per_level[i] = tdl_port.TDLPort(
          name="lvl%i-port" %
          (i))

    # initialize BCube 0 objects
    self.bcube_0_server = tdl_endhost.TDLEndhost(name="server")
    for server_port_ct in self.server_port_ct_per_level.itervalues():
      self.bcube_0_server.Contains(server_port_ct, 1)

    # initialize the recursive DCell structure
    self.top_level_dcell = BCube(level=self.top_level_idx, container=self)
    self.Contains(self.top_level_dcell, 1)


class BCubeSwitch(tdl_switching_component.TDLSwitchingComponent):

  def __init__(self, num_ports):

    super(BCubeSwitch, self).__init__(name="switch")
    self.port = tdl_port.TDLPort(name="port")
    self.Contains(self.port, num_ports)


class BCube(tdl_component.TDLComponent):

  def __init__(self, level, container):

    super(BCube, self).__init__(name="bcube_lvl%i_" % level)

    self.bcube_level = level  # used for external inspection
    self.bcube_sw_tpl = BCubeSwitch(container.num_ports_bcube_sw)
    self.Contains(self.bcube_sw_tpl, pow(container.num_ports_bcube_sw, level))

    if level == 0:
      # add servers to the hierarchy, setup connectivity
      self.Contains(container.bcube_0_server, container.num_servers_bcube_0)

      constraints = []
      pairCSPCstr_lib.EveryCmptPairMustHaveExactlyXConnections(
          constraint_set=constraints,
          ct_1=self.bcube_sw_tpl,
          ct_2=container.bcube_0_server,
          num_connections=1)

      connector = connectorCSP_lib.PhysicalConnector()
      connector.AddCTPairToConnect(self.bcube_sw_tpl,
                                   container.bcube_0_server,
                                   self.bcube_sw_tpl.port,
                                   container.server_port_ct_per_level[0])
      connector.AddConstraints(constraints)
      self.AddConnector(connector)

    else:
      # add sub-DCells, setup connectivity at level 1+
      sub_bcube = BCube(level - 1, container)
      self.Contains(sub_bcube, container.num_sub_bcubes_per_bcube)

      constraints = []
      pairCSPCstr_lib.EveryCmptPairMustHaveExactlyXConnections(
          constraint_set=constraints,
          ct_1=self.bcube_sw_tpl,
          ct_2=sub_bcube,
          num_connections=1)

      connector = connectorCSP_lib.PhysicalConnector()
      connector.AddCTPairToConnect(self.bcube_sw_tpl,
                                   sub_bcube,
                                   self.bcube_sw_tpl.port,
                                   container.server_port_ct_per_level[level])
      connector.AddConstraints(constraints)
      self.AddConnector(connector)
