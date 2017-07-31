# coding=utf-8
DEFAULT_MAX_METADATA_SIZE = 10 * 1024 * 1024
BOOTSTRAPPING_NODES = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881)
]
TRANSPORT_BUFFER_SIZE = 5000000

EXCLUDE = [
    '100.64.0.0/10',
    '10.0.0.0/8',
    '172.16.0.0/12',
    '192.168.0.0/16'
]
