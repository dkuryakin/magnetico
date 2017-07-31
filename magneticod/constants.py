# coding=utf-8
DEFAULT_MAX_METADATA_SIZE = 10 * 1024 * 1024
BOOTSTRAPPING_NODES = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881)
]
TRANSPORT_BUFFER_SIZE = 5000000

 # maximum (inclusive) number of active (disposable) peers to fetch the metadata per info hash at the same time:
MAX_ACTIVE_PEERS_PER_INFO_HASH = 5

PEER_TIMEOUT = 30  # seconds

EXCLUDE = [
    '100.64.0.0/10',
    '10.0.0.0/8',
    '172.16.0.0/12',
    '192.168.0.0/16'
]
