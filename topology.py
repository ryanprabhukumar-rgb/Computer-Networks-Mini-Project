from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel

def build_network():
    net = Mininet(controller=RemoteController, switch=OVSKernelSwitch)
    net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633)
    
    s1 = net.addSwitch('s1')
    h1 = net.addHost('h1', ip='10.0.0.1/24', mac='00:00:00:00:00:01')
    h2 = net.addHost('h2', ip='10.0.0.2/24', mac='00:00:00:00:00:02')
    h3 = net.addHost('h3', ip='10.0.0.3/24', mac='00:00:00:00:00:03')

    for h in (h1, h2, h3):
        net.addLink(h, s1)

    nat = net.addNAT()

    net.start()
    nat.configDefault()

    h1.cmd('python3 manager.py &')
    h2.cmd('python3 worker.py 10.0.0.1 Worker_1 &')
    h3.cmd('python3 worker.py 10.0.0.1 Worker_2 &')

    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    build_network()
