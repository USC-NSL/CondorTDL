"""Description of a FatTree Topology with Condor's TDL

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

FatTree is a tree topology, described in the following SIGCOMM paper
http://ccr.sigcomm.org/online/files/p63-alfares.pdf
"""

from condor.tdl import tdl_component
from condor.tdl import tdl_switching_component
from condor.tdl import tdl_port

from condor.connect.physical import connector as connectorCSP_lib
from condor.connect.physical.constraints import pairwise as pairCSPCstr_lib


class FatTreeSwitchQx40GE(tdl_switching_component.TDLSwitchingComponent):

  def __init__(self, num_ports, name="switch"):
    super(FatTreeSwitchQx40GE, self).__init__(name=name)
    self.port = tdl_port.TDLPort("port40GE-", tdl_port.TDLPort.Speed.FOURTYGBPS)
    self.num_ports = num_ports
    self.Contains(self.port, self.num_ports)


class FatTree(tdl_component.TDLComponent):

  def __init__(self, num_pods):

    super(FatTree, self).__init__(name="fattree")

    # parameters / calculations
    # (from equations in FatTree paper)
    self.num_pods = num_pods
    self.num_ports_per_sw = self.num_pods
    self.num_sw_pod_layer = self.num_pods / 2
    self.num_spine_sw = pow((self.num_pods / 2), 2)

    # pod description
    self.agg_sw = FatTreeSwitchQx40GE(self.num_ports_per_sw, "agg-sw")
    self.tor_sw = FatTreeSwitchQx40GE(self.num_ports_per_sw, "tor-sw")
    self.pod = tdl_component.TDLComponent("pod")
    self.pod.Contains(self.agg_sw, self.num_sw_pod_layer)
    self.pod.Contains(self.tor_sw, self.num_sw_pod_layer)

    # pod connectivity description
    constraints = []
    # in each pod, an agg connects to all ToRs
    pairCSPCstr_lib.EveryCmptPairMustHaveExactlyXConnections(
        constraint_set=constraints,
        ct_1=self.agg_sw,
        ct_2=self.tor_sw,
        num_connections=1)
    connector = connectorCSP_lib.PhysicalConnector()
    connector.AddCTPairToConnect(self.agg_sw,
                                 self.tor_sw,
                                 self.agg_sw.port,
                                 self.tor_sw.port)
    connector.AddConstraints(constraints)
    self.pod.AddConnector(connector)

    # spine description
    self.spine_sw = FatTreeSwitchQx40GE(self.num_ports_per_sw, "spine-sw")

    # add spines and pods
    self.Contains(self.spine_sw, self.num_spine_sw)
    self.Contains(self.pod, self.num_pods)

    # setup connectivity between spines and pods
    constraints = []
    # a pod connects to every spine switch once
    pairCSPCstr_lib.EveryCmptPairMustHaveExactlyXConnections(
        constraint_set=constraints,
        ct_1=self.spine_sw,
        ct_2=self.pod,
        num_connections=1)

    # setup the connector for spine <-> pod connectivity
    # (note, we can specify at pod level to reduce # of vars required, this is
    #  possible because constraints are not specified at the agg level)
    connector = connectorCSP_lib.PhysicalConnector()
    connector.AddCTPairToConnect(self.pod,
                                 self.spine_sw,
                                 self.agg_sw.port,
                                 self.spine_sw.port)
    connector.AddConstraints(constraints)
    self.AddConnector(connector)
