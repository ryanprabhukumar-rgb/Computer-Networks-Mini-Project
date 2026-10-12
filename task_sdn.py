from pox.core import core
import pox.openflow.libopenflow_01 as of

log = core.getLogger()
APP_PORT = 9090  # Task Orchestrator TCP Socket Port

class TaskSDNController(object):
    def __init__(self, connection):
        self.connection = connection
        self.mac_to_port = {}
        connection.addListeners(self)
        log.info("[SDN CONTROLLER] Switch Connected - Custom Task SDN Active")

    def _handle_PacketIn(self, event):
        packet = event.parsed
        if not packet.parsed:
            return

        self.mac_to_port[packet.src] = event.port
        out_port = self.mac_to_port.get(packet.dst, of.OFPP_FLOOD)

        tcp = packet.find("tcp")
        is_task_traffic = bool(tcp and (tcp.srcport == APP_PORT or tcp.dstport == APP_PORT))

        fm = of.ofp_flow_mod()
        fm.match = of.ofp_match.from_packet(packet, event.port)
        fm.idle_timeout = 30

        if is_task_traffic:
            fm.priority = 100  # High-priority rule for task application traffic
            log.info(f"[SDN RULE] TCP Port {APP_PORT} Task Traffic Detected -> Installed Flow Rule (Priority 100)")
        else:
            fm.priority = 10   # Standard priority for background network traffic

        fm.actions.append(of.ofp_action_output(port=out_port))

        if out_port != of.OFPP_FLOOD:
            self.connection.send(fm)

        msg = of.ofp_packet_out(data=event.ofp, in_port=event.port)
        msg.actions.append(of.ofp_action_output(port=out_port))
        self.connection.send(msg)

def launch():
    core.openflow.addListenerByName("ConnectionUp", lambda e: TaskSDNController(e.connection))
    log.info("[SDN CONTROLLER] Task SDN Module Initialized")
