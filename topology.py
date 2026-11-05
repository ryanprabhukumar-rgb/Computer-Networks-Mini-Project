"""
Mininet topology for EdgeFlow, connected to the remote POX controller.

  h1 10.0.0.1  Task Manager (+ dashboard)        h5 10.0.0.5  noisy / compromised host
  h2 10.0.0.2  Worker_1                          h6 10.0.0.6  rogue host (unauthorized access)  
  h3 10.0.0.3  Worker_2                          nat0 10.0.0.254  lets the VM browser reach h1:5000
  h4 10.0.0.4  Worker_3

Each host link is limited to 10 Mbit/s with TCLink for the congestion tests.
"""
from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.log import setLogLevel
import json
import os

CHEATSHEET = """
*** EdgeFlow ready. Dashboard (VM browser): http://10.0.0.1:5000
    Network scenarios (run at the mininet> prompt):
      scenarios        : buttons on the dashboard (Worker failure, Link failure / Restore link, Unauthorized access)
      link failure     : 'Link failure' button, or  link s1 h2 down  (restore: link s1 h2 up)
      unauthorized host: 'Unauthorized access' button, or  h6 python3 rogue.py 10.0.0.1
      flow table       : sh ovs-ofctl dump-flows s1
      evaluation       : 'Run evaluation' button on the dashboard (or h1 python3 evaluate.py)
      logs             : h1 tail -f /tmp/manager.log      h2 tail /tmp/worker1.log
"""


def build_network():
    # Create the test network and connect the switch to the POX controller.
    # The bandwidth limit lets the demo generate measurable congestion.
    net = Mininet(controller=RemoteController, switch=OVSKernelSwitch, link=TCLink)
    net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633)

    s1 = net.addSwitch('s1')
    hosts = {}
    for i in range(1, 7):
        hosts[i] = net.addHost(f'h{i}', ip=f'10.0.0.{i}/24', mac=f'00:00:00:00:00:0{i}')
        net.addLink(hosts[i], s1, bw=10)

    # Start Mininet and let the VM browser reach the manager dashboard through NAT.
    nat = net.addNAT()
    net.start()
    nat.configDefault()

    # The dashboard uses these PIDs when it triggers host or link test actions.
    with open('/tmp/edgeflow_hosts.json', 'w') as f:
        json.dump({**{h.name: h.pid for h in net.hosts}, 'root': os.getpid()}, f)

    # Start the manager and workers in the background, with separate logs for debugging/demo.
    hosts[1].cmd('python3 manager.py > /tmp/manager.log 2>&1 &')
    for host, wid, log in ((hosts[2], 'Worker_1', 'worker1'), (hosts[3], 'Worker_2', 'worker2'),
                           (hosts[4], 'Worker_3', 'worker3')):
        host.cmd(f'python3 worker.py 10.0.0.1 {wid} > /tmp/{log}.log 2>&1 &')

    print(CHEATSHEET)
    CLI(net)
    net.stop()


if __name__ == '__main__':
    setLogLevel('info')
    build_network()
