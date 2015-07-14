"""Description of a DCell Topology with Condor's TDL

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

DCell is a recursive topology, described in the following SIGCOMM paper
http://www.msr-waypoint.com/pubs/75988/dcell.pdf
"""

from condor.tdl import tdl_switching_component
from condor.tdl import tdl_component
from condor.tdl import tdl_endhost
from condor.tdl import tdl_port

from condor.connect.physical import connector as connectorCSP_lib
from condor.connect.physical.constraints import pairwise as pairCSPCstr_lib

# equations from DCell paper that define topology's recursive structure


def ReturnTk(k, n):
  if k == 0:
    return n
  return ReturnGk(k, n) * ReturnTk(k - 1, n)


def ReturnGk(k, n):
  if k == 0:
    return 1
  return ReturnTk(k - 1, n) + 1


class DCellContainer(tdl_component.TDLComponent):

  def __init__(self, k, n):

    # initialize the underlying ComponentTemplate, store information
    super(DCellContainer, self).__init__(name="dcell")
    self.top_level_idx = k
    self.num_servers_dcell_0 = n
    self.num_ports_dcell_0_sw = n

    # initialize port objects used to balance DCell connectivity at each level
    self.server_port_ct_per_level = {}
    for i in xrange(0, self.top_level_idx + 1):
      self.server_port_ct_per_level[i] = tdl_port.TDLPort(
          name="lvl%i-port" %
          (i))

    # initialize DCell 0 objects
    self.dcell_0_sw_port = tdl_port.TDLPort(name="port")
    self.dcell_0_sw = tdl_switching_component.TDLSwitchingComponent("switch")
    self.dcell_0_sw.Contains(self.dcell_0_sw_port, self.num_ports_dcell_0_sw)
    self.dcell_0_server = tdl_endhost.TDLEndhost(name="server")
    for server_port_ct in self.server_port_ct_per_level.itervalues():
      self.dcell_0_server.Contains(server_port_ct, 1)

    # initialize the recursive DCell structure
    self.top_level_dcell = DCell(level=self.top_level_idx, container=self)
    self.Contains(self.top_level_dcell, 1)


class DCell(tdl_component.TDLComponent):

  def __init__(self, level, container):

    super(DCell, self).__init__(name="dcell_lvl%i_" % level)

    self.dcell_level = level  # used for external inspection
    if level == 0:
      # add switches and servers to the hierarchy, setup connectivity
      self.Contains(container.dcell_0_server, container.num_servers_dcell_0)
      self.Contains(container.dcell_0_sw, 1)

      constraints = []
      pairCSPCstr_lib.EveryCmptPairMustHaveExactlyXConnections(
          constraint_set=constraints,
          ct_1=container.dcell_0_server,
          ct_2=container.dcell_0_sw,
          num_connections=1)

      connector = connectorCSP_lib.PhysicalConnector()
      connector.AddCTPairToConnect(container.dcell_0_server,
                                   container.dcell_0_sw,
                                   container.server_port_ct_per_level[0],
                                   container.dcell_0_sw_port)
      connector.AddConstraints(constraints)
      self.AddConnector(connector)

    else:
      # add sub-DCells, setup connectivity at level 1+
      sub_dcell = DCell(level - 1, container)
      num_sub_dcell = ReturnGk(level, container.num_servers_dcell_0)
      self.Contains(sub_dcell, num_sub_dcell)

      constraints = []
      pairCSPCstr_lib.EveryCmptPairMustHaveExactlyXConnections(
          constraint_set=constraints,
          ct_1=sub_dcell,
          ct_2=sub_dcell,
          num_connections=1)

      connector = connectorCSP_lib.PhysicalConnector()
      connector.AddCTPairToConnect(sub_dcell,
                                   sub_dcell,
                                   container.server_port_ct_per_level[level],
                                   container.server_port_ct_per_level[level])
      connector.AddConstraints(constraints)
      self.AddConnector(connector)
