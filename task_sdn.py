"""
task_sdn.py - POX controller for EdgeFlow.

Run:  cd ~/pox && PYTHONPATH=$HOME/d2 python3 pox.py task_sdn

What it does
  FORWARDING   learning switch; TCP 9090 (task traffic) gets priority-100 rules, other traffic priority 10
  APP -> SDN   the manager sends small UDP events to the virtual address 10.0.0.100:9999
                 LOAD (worker workload) | REG (worker registered) | DOWN (worker failed)
                 UNAUTH (host rejected by the manager) | HELLO (every second, carries the scheduling policy)
  SDN -> APP   the controller writes the network state of every worker to NET_FILE; the manager reads it
  MONITORING   every POLL_SECONDS it reads flow statistics (traffic per worker)
  CONGESTION   non-task traffic into a worker above CONGESTION_BPS -> worker is CONGESTED and the heaviest
               offending flow is dropped for BLOCK_SECONDS (only when the manager runs the dynamic policy)
  LINK FAILURE port goes down -> worker is DOWN, its flows are removed, the manager is told at once
  BLOCKING     host rejected by the manager -> TCP 9090 from that host is dropped
  RECOVERY     port up / worker registers again -> state cleared
"""
import json
import os
import time

from pox.core import core
import pox.openflow.libopenflow_01 as of
from pox.lib.addresses import IPAddr, EthAddr
from pox.lib.packet.ethernet import ethernet
from pox.lib.packet.arp import arp
from pox.lib.recoco import Timer

log = core.getLogger()

APP_PORT = 9090                                   # task traffic (manager TCP port)
CTRL_VIP = IPAddr("10.0.0.100")                   # virtual controller address (we answer ARP for it)
CTRL_MAC = EthAddr("02:00:00:00:01:00")
CTRL_PORT = 9999                                  # UDP port for application events
NET_FILE = "/tmp/edgeflow_net.json"               # controller -> manager feedback
POLL_SECONDS = 2
CONGESTION_BPS = 400000                           # bytes/s of non-task traffic into one worker (~3.2 Mbit/s)
BLOCK_SECONDS = 20                                # lifetime of a congestion drop rule


class TaskSDN(object):
    def __init__(self, connection):
        self.connection = connection
        self.mac_to_port = {}                     # learning switch table
        self.host_port = {}                       # ip -> switch port
        self.down_ports = set()
        self.workers = {}                         # worker id -> dict(ip, up, congested, other)
        self.blocked = {}                         # ip -> reason
        self.mitigations = []                     # recent congestion actions (text)
        self.prev_bytes = {}                      # flow -> (bytes, time) for rate calculation
        self.dynamic = True                       # False while the manager runs the static baseline
        connection.addListeners(self)
        self.timer = Timer(POLL_SECONDS, self._poll_stats, recurring=True)
        log.info("[SDN CONTROLLER] Switch connected - EdgeFlow task SDN active")

    def _handle_ConnectionDown(self, event):
        self.timer.cancel()

    # ------------------------------------------------------------ SDN -> manager
    def _worker_state(self, w):
        if self.host_port.get(w["ip"]) in self.down_ports:
            return "DOWN"                         # physical link failure
        if not w["up"]:
            return "OFFLINE"                      # manager says it disconnected
        return "CONGESTED" if w["congested"] else "OK"

    def _save_state(self):
        """Write the network state of every worker to a file; the manager reads it every 0.25 s."""
        state = {"time": time.time(), "blocked": sorted(self.blocked), "mitigations": self.mitigations[-3:],
                 "workers": {wid: {"s": self._worker_state(w), "r": round(w["other"] / 1000.0, 1)}
                             for wid, w in self.workers.items()}}
        try:
            with open(NET_FILE + ".tmp", "w") as f:
                json.dump(state, f)
            os.replace(NET_FILE + ".tmp", NET_FILE)       # atomic: the manager never reads half a file
        except OSError as e:
            log.warning("[SDN CONTROLLER] cannot write %s: %s", NET_FILE, e)

    # ----------------------------------------------------- manager -> SDN (events)
    def _handle_app_event(self, payload):
        try:
            msg = json.loads(bytes(payload).decode().strip())
        except ValueError:
            return
        kind, wid = msg.get("e"), msg.get("w")
        if kind == "HELLO":
            self.dynamic = msg.get("p") != "round_robin"
        elif kind == "UNAUTH":
            self._block(str(msg.get("ip")), msg.get("r", "rejected by the manager"))
        elif wid and kind in ("LOAD", "REG", "DOWN"):
            w = self.workers.setdefault(wid, dict(ip=msg.get("ip"), up=True, congested=False, other=0.0))
            w["ip"] = msg.get("ip", w["ip"])
            w["up"] = (kind == "REG") or (kind == "LOAD" and msg.get("s") == "UP")
            if kind == "REG":
                w["congested"] = False
                log.info("[SDN LOAD] %s registered (%s) - state cleared", wid, w["ip"])
            elif kind == "DOWN":
                log.info("[SDN FAILURE] manager reports %s DOWN - removing its flows", wid)
                for field in ("nw_dst", "nw_src"):
                    self.connection.send(of.ofp_flow_mod(command=of.OFPFC_DELETE,
                                                         match=of.ofp_match(dl_type=0x0800, **{field: IPAddr(w["ip"])})))
            else:
                log.debug("[SDN LOAD] %s outstanding=%s done=%s", wid, msg.get("o"), msg.get("d"))
        if kind != "LOAD":
            self._save_state()

    def _block(self, ip_text, reason):
        if ip_text in self.blocked:
            return
        self.blocked[ip_text] = reason
        addr = IPAddr(ip_text)
        self.connection.send(of.ofp_flow_mod(command=of.OFPFC_DELETE, match=of.ofp_match(dl_type=0x0800, nw_src=addr)))
        fm = of.ofp_flow_mod()                                # no actions = drop
        fm.match = of.ofp_match(dl_type=0x0800, nw_proto=6, nw_src=addr, tp_dst=APP_PORT)
        fm.priority = 200
        self.connection.send(fm)
        log.warning("[SDN BLOCK] %s blocked from TCP %d: %s", ip_text, APP_PORT, reason)
        self._save_state()

    # ----------------------------------------------------------------- monitoring
    def _poll_stats(self):
        self.connection.send(of.ofp_stats_request(body=of.ofp_flow_stats_request()))

    def _handle_FlowStatsReceived(self, event):
        now, rate_to, top_flow = time.time(), {}, {}      # rate_to[ip] = non-task bytes/s towards that host
        for f in event.stats:
            m = f.match
            if m.nw_src is None or m.nw_dst is None or APP_PORT in (m.tp_src, m.tp_dst) or f.priority == 150:
                continue
            key = (str(m.nw_src), str(m.nw_dst), m.nw_proto, m.tp_src, m.tp_dst, m.in_port)
            prev = self.prev_bytes.get(key)
            self.prev_bytes[key] = (f.byte_count, now)
            if not prev or f.byte_count < prev[0] or now <= prev[1]:
                continue                                  # new flow or counter reset
            rate = (f.byte_count - prev[0]) / (now - prev[1])
            dst = str(m.nw_dst)
            rate_to[dst] = rate_to.get(dst, 0.0) + rate
            if rate > top_flow.get(dst, (0, None))[0]:
                top_flow[dst] = (rate, m)
        for wid, w in self.workers.items():
            w["other"] = rate_to.get(w["ip"], 0.0)
            was = w["congested"]
            w["congested"] = self.dynamic and w["other"] > CONGESTION_BPS
            if w["congested"] and not was:
                log.warning("[SDN MONITOR] %s congested: %.0f kB/s of non-task traffic", wid, w["other"] / 1000)
            if w["congested"]:
                self._mitigate(wid, top_flow[w["ip"]])
        self._save_state()

    def _mitigate(self, wid, offender):
        """Drop the heaviest non-task flow into the congested worker for BLOCK_SECONDS."""
        rate, m = offender
        fm = of.ofp_flow_mod()                            # no actions = drop
        fm.match = of.ofp_match(dl_type=0x0800, nw_proto=m.nw_proto, nw_src=m.nw_src, nw_dst=m.nw_dst,
                                tp_src=m.tp_src, tp_dst=m.tp_dst)
        fm.priority, fm.hard_timeout = 150, BLOCK_SECONDS
        self.connection.send(fm)
        text = "%s -> %s %.0f kB/s dropped %ds" % (m.nw_src, m.nw_dst, rate / 1000, BLOCK_SECONDS)
        self.mitigations.append(text)
        log.warning("[SDN MITIGATE] protecting %s: %s", wid, text)

    def _handle_PortStatus(self, event):
        port, desc = event.port, event.ofp.desc
        down = event.deleted or bool(desc.state & of.OFPPS_LINK_DOWN) or bool(desc.config & of.OFPPC_PORT_DOWN)
        if down and port not in self.down_ports:
            self.down_ports.add(port)
            log.warning("[SDN FAILURE] port %d went DOWN - removing flows and learned hosts on it", port)
            self.connection.send(of.ofp_flow_mod(command=of.OFPFC_DELETE, out_port=port))
            self.connection.send(of.ofp_flow_mod(command=of.OFPFC_DELETE, match=of.ofp_match(in_port=port)))
            self.mac_to_port = {m: p for m, p in self.mac_to_port.items() if p != port}
        elif not down and port in self.down_ports:
            self.down_ports.discard(port)
            log.info("[SDN RECOVERY] port %d is UP again", port)
        self._save_state()

    # ------------------------------------------------------------------ data plane
    def _handle_PacketIn(self, event):
        packet = event.parsed
        if not packet.parsed:
            return
        self.mac_to_port[packet.src] = event.port
        ip, tcp, udp, a = packet.find("ipv4"), packet.find("tcp"), packet.find("udp"), packet.find("arp")
        if ip:
            self.host_port[str(ip.srcip)] = event.port

        if a and a.opcode == arp.REQUEST and a.protodst == CTRL_VIP:      # ARP for the virtual address
            reply = arp(opcode=arp.REPLY, hwsrc=CTRL_MAC, hwdst=a.hwsrc, protosrc=CTRL_VIP, protodst=a.protosrc)
            eth = ethernet(type=ethernet.ARP_TYPE, src=CTRL_MAC, dst=a.hwsrc)
            eth.payload = reply
            out = of.ofp_packet_out(data=eth.pack(), in_port=of.OFPP_NONE)
            out.actions.append(of.ofp_action_output(port=event.port))
            return self.connection.send(out)

        if ip and udp and ip.dstip == CTRL_VIP and udp.dstport == CTRL_PORT:   # application event
            return self._handle_app_event(udp.payload)

        if ip and tcp and tcp.dstport == APP_PORT and str(ip.srcip) in self.blocked:
            return                                                     # blocked host: drop silently

        out_port = self.mac_to_port.get(packet.dst, of.OFPP_FLOOD)
        if out_port in self.down_ports:
            return                                                     # never forward into a failed link
        if out_port == of.OFPP_FLOOD or packet.dst.is_multicast:
            msg = of.ofp_packet_out(data=event.ofp, in_port=event.port)
            msg.actions.append(of.ofp_action_output(port=of.OFPP_FLOOD))
            return self.connection.send(msg)

        is_task = bool(tcp and APP_PORT in (tcp.srcport, tcp.dstport))
        fm = of.ofp_flow_mod()
        fm.match = of.ofp_match.from_packet(packet, event.port)
        fm.idle_timeout = 30
        fm.priority = 100 if is_task else 10                           # task traffic gets the high-priority rule
        fm.actions.append(of.ofp_action_output(port=out_port))
        fm.data = event.ofp                                            # also forwards the waiting packet
        self.connection.send(fm)
        if is_task:
            log.info("[SDN RULE] TCP %d task traffic %s -> %s: flow rule installed (priority 100)",
                     APP_PORT, ip.srcip, ip.dstip)


def launch():
    core.openflow.miss_send_len = 0x7fff        # default is 128 bytes: application events would be cut off
    core.openflow.addListenerByName("ConnectionUp", lambda e: TaskSDN(e.connection))
    log.info("[SDN CONTROLLER] EdgeFlow Task SDN module initialised")

