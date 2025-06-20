# server.py
import logging
import threading

debugging = False

def debug(format, *args):
    if debugging:
        logging.info(format % args)

class PutAppendArgs:
    def __init__(self, key, value, client_id=None, seq_id=None):
        self.key = key
        self.value = value
        self.client_id = client_id
        self.seq_id = seq_id

class PutAppendReply:
    def __init__(self, value):
        self.value = value

class GetArgs:
    def __init__(self, key, client_id=None, seq_id=None):
        self.key = key
        self.client_id = client_id
        self.seq_id = seq_id

class GetReply:
    def __init__(self, value):
        self.value = value

class KVServer:
    def __init__(self, cfg, srvid):
        self.mu = threading.Lock()
        self.cfg = cfg
        self.srvid = srvid
        # In-memory key/value store
        self.kv = {}
        # Duplicate detection state: last seq seen and its reply per client
        self.last_request = {}  # client_id -> last seq_id
        self.last_reply = {}    # client_id -> reply object

    def is_responsible(self, key: str) -> bool:
        total = len(self.cfg.kvservers)
        # single-shard handles all keys
        if total == 1:
            return True
        try:
            shard = int(key) % total
        except ValueError:
            # non-integer keys go to shard 0
            shard = 0
        distance = (self.srvid - shard + total) % total
        return distance < self.cfg.nreplicas

    def Get(self, args: GetArgs) -> GetReply:
        # Reject if not responsible
        if not self.is_responsible(args.key):
            # never reply
            threading.Event().wait()
        with self.mu:
            last_seq = self.last_request.get(args.client_id, -1)
            if args.seq_id <= last_seq:
                return self.last_reply[args.client_id]
            value = self.kv.get(args.key, "")
            reply = GetReply(value)
            self.last_request[args.client_id] = args.seq_id
            self.last_reply[args.client_id] = reply
            return reply

    def Put(self, args: PutAppendArgs) -> PutAppendReply:
        if not self.is_responsible(args.key):
            threading.Event().wait()
        with self.mu:
            last_seq = self.last_request.get(args.client_id, -1)
            if args.seq_id <= last_seq:
                return self.last_reply[args.client_id]
            self.kv[args.key] = args.value
            reply = PutAppendReply(None)
            self.last_request[args.client_id] = args.seq_id
            self.last_reply[args.client_id] = reply
            return reply

    def Append(self, args: PutAppendArgs) -> PutAppendReply:
        if not self.is_responsible(args.key):
            threading.Event().wait()
        with self.mu:
            last_seq = self.last_request.get(args.client_id, -1)
            if args.seq_id <= last_seq:
                return self.last_reply[args.client_id]
            old = self.kv.get(args.key, "")
            self.kv[args.key] = old + args.value
            reply = PutAppendReply(old)
            self.last_request[args.client_id] = args.seq_id
            self.last_reply[args.client_id] = reply
            return reply