import logging
import struct
import socket
import time
from random import randint

logger = logging.getLogger(__package__)
logging.basicConfig(level=logging.CRITICAL)  # Ensure you see critical logs

class RPCProtocolError(Exception):
    pass


class RPC(object):
    connections = list()

    def __init__(self, host, port, timeout):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.client = None
        self.client_port = None

    def request(self, program, program_version, procedure, data=None, message_type=0, version=2, auth=None):
        logger.debug(f"RPC.request: Preparing request to {self.host}:{self.port}, procedure={procedure}")
        rpc_xid = int(time.time())
        rpc_message_type = message_type     # 0=call
        rpc_rpc_version = version
        rpc_program = program
        rpc_program_version = program_version
        rpc_procedure = procedure
        rpc_verifier_flavor = 0             # AUTH_NULL
        rpc_verifier_length = 0

        proto = struct.pack('!LLLLLL', rpc_xid, rpc_message_type, rpc_rpc_version, rpc_program, rpc_program_version, rpc_procedure)

        if auth is None:    # AUTH_NULL
            proto += struct.pack('!LL', 0, 0)
        elif auth["flavor"] == 1:   # AUTH_UNIX
            stamp = int(time.time()) & 0xffff
            auth_data = struct.pack("!LL", stamp, len(auth["machine_name"]))
            auth_data += auth["machine_name"].encode()
            auth_data += b'\x00'*((4-len(auth["machine_name"]) % 4) % 4)
            auth_data += struct.pack("!LL", auth["uid"], auth["gid"])
            if len(auth['aux_gid']) == 1 and auth['aux_gid'][0] == 0:
                auth_data += struct.pack("!L", 0)
            else:
                auth_data += struct.pack("!L", len(auth["aux_gid"]))
                for aux_gid in auth["aux_gid"]:
                    auth_data += struct.pack("!L", aux_gid)
            proto += struct.pack('!LL', 1, len(auth_data))
            proto += auth_data
        else:
            raise Exception("RPC unknown auth method")
        proto += struct.pack('!LL', rpc_verifier_flavor, rpc_verifier_length)

        if data is not None:
            proto += data

        rpc_fragment_header = 0x80000000 + len(proto)
        proto = struct.pack('!L', rpc_fragment_header) + proto

        try:
            logger.debug(f"RPC.request: Sending request ({len(proto)} bytes)")
            self.client.send(proto)

            last_fragment = False
            data = b""

            while not last_fragment:
                response = self.recv()
                last_fragment = struct.unpack('!L', response[:4])[0] & 0x80000000 != 0
                data += response[4:]

            rpc = data[:24]
            (
                rpc_XID,
                rpc_Message_Type,
                rpc_Reply_State,
                rpc_Verifier_Flavor,
                rpc_Verifier_Length,
                rpc_Accept_State
            ) = struct.unpack('!LLLLLL', rpc)

            logger.debug(f"RPC.request: Received reply, Message_Type={rpc_Message_Type}, Accept_State={rpc_Accept_State}")

            if rpc_Message_Type != 1 or rpc_Reply_State != 0 or rpc_Accept_State != 0:
                raise Exception("RPC protocol error")

            data = data[24:]
        except Exception as e:
            # logger.exception("Exception during RPC.request:")
            # still raise the exception to be handled by the caller
            raise RPCProtocolError(f"Error in RPC request: {e}")
        return data

    def connect(self):
        logger.debug(f"Connecting to {self.host}:{self.port} with timeout {self.timeout}")
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.settimeout(self.timeout)
        random_port = None
        try:
            i = 0
            while True:
                try:
                    random_port = randint(10000, 11000) # changed this range to enable non-privileged ports
                    i += 1
                    self.client.bind(('', random_port))
                    self.client_port = random_port
                    logger.debug("Port %d occupied" % self.client_port)
                    break
                except Exception as e:
                    logger.warning(f"Socket port binding with {random_port} failed in loop {i}, try again. {e}")
                    continue
        except Exception as e:
            logger.error(f"[ERROR] Error in port binding: {e}")

        self.client.connect((self.host, self.port))
        logger.debug(f"Connected to {self.host}:{self.port}")
        RPC.connections.append(self)

    def disconnect(self):
        logger.debug("Disconnecting socket.")
        self.client.close()
        logger.debug("Port %s released" % self.client_port)

    @classmethod
    def disconnect_all(cls):
        counter = 0
        for item in cls.connections:
            try:
                item.client.close()
                counter += 1
            except Exception as e:
                logger.warning(f"Error disconnecting socket: {e}")
        logger.debug("Disconnect all connecting rpc sockets, amount: %d" % counter)

    def recv(self):
        rpc_response_size = b""

        try:
            while len(rpc_response_size) != 4:
                chunk = self.client.recv(4 - len(rpc_response_size))
                if not chunk:
                    raise RPCProtocolError("Connection closed by server while reading header")
                rpc_response_size += chunk

            if len(rpc_response_size) != 4:
                raise RPCProtocolError("incorrect recv response size: %d" % len(rpc_response_size))
            response_size = struct.unpack('!L', rpc_response_size)[0] & 0x7fffffff

            rpc_response = rpc_response_size
            while len(rpc_response) < response_size:
                chunk_size = response_size - len(rpc_response) + 4
                
                chunk = self.client.recv(chunk_size)
                if not chunk:
                    raise RPCProtocolError("Connection closed by server")
                rpc_response = rpc_response + chunk

            return rpc_response
        except Exception as e:
            # logger.exception(e)
            # but still raise the exception to be handled by the caller
            raise RPCProtocolError(f"Error receiving data: {e}")
