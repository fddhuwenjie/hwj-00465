import json
import struct


def encode_message(data):
    payload = json.dumps(data, ensure_ascii=False).encode('utf-8')
    length = struct.pack('!I', len(payload))
    return length + payload


def decode_message(data):
    if len(data) < 4:
        return None, data
    length = struct.unpack('!I', data[:4])[0]
    if len(data) < 4 + length:
        return None, data
    payload = data[4:4 + length]
    remaining = data[4 + length:]
    try:
        msg = json.loads(payload.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError):
        msg = None
    return msg, remaining


def recv_message(sock):
    buf = b''
    while True:
        try:
            chunk = sock.recv(4096)
        except Exception:
            return None
        if not chunk:
            return None
        buf += chunk
        msg, buf = decode_message(buf)
        if msg is not None:
            return msg


def send_message(sock, data):
    try:
        sock.sendall(encode_message(data))
        return True
    except Exception:
        return False
