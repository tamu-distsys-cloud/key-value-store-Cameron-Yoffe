# client.py
# Used ChatGPT to help comment on the modifications I could make to complete part 3 of the assignment.
import random
from typing import List

from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

def nrand() -> int:
    return random.getrandbits(62)

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg
        self.client_id = nrand()
        self.seq_id = 0
        self.nservers = len(servers)
        self.nreplicas = cfg.nreplicas

    def get(self, key: str) -> str:
        self.seq_id += 1
        args = GetArgs(key, self.client_id, self.seq_id)
        total = self.nservers
        # determine primary shard
        if total > 1:
            try:
                shard = int(key) % total
            except ValueError:
                shard = 0
        else:
            shard = 0
        primary = shard
        # if network reliable and multi-replica, allow fallback to replicas
        if getattr(self.cfg, 'reliable', True) and self.nreplicas > 1:
            while True:
                for i in range(self.nreplicas):
                    srv = (shard + i) % total
                    try:
                        reply = self.servers[srv].call("KVServer.Get", args)
                        return reply.value
                    except TimeoutError:
                        continue
        # else, only query primary
        while True:
            try:
                reply = self.servers[primary].call("KVServer.Get", args)
                return reply.value
            except TimeoutError:
                continue

    def put_append(self, key: str, value: str, op: str) -> str:
        self.seq_id += 1
        args = PutAppendArgs(key, value, self.client_id, self.seq_id)
        total = self.nservers
        if total > 1:
            try:
                shard = int(key) % total
            except ValueError:
                shard = 0
        else:
            shard = 0
        primary = shard
        while True:
            try:
                primary_reply = self.servers[primary].call(f"KVServer.{op}", args)
                break
            except TimeoutError:
                continue
        for i in range(1, self.nreplicas):
            srv = (shard + i) % total
            while True:
                try:
                    # ignore reply
                    _ = self.servers[srv].call(f"KVServer.{op}", args)
                    break
                except TimeoutError:
                    continue
        # return primary's reply
        if op == "Append":
            return primary_reply.value
        return ""

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")