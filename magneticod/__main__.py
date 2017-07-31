# magneticod - Autonomous BitTorrent DHT crawler and metadata fetcher.
# Copyright (C) 2017  Mert Bora ALPER <bora@boramalper.org>
# Dedicated to Cemile Binay, in whose hands I thrived.
#
# This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License along with this program.  If not, see
# <http://www.gnu.org/licenses/>.
import argparse
import asyncio
import logging
import ipaddress
import textwrap
import urllib.parse
import os
import sys
import typing

import appdirs
import humanfriendly

from .constants import DEFAULT_MAX_METADATA_SIZE
from . import __version__
from . import dht
from . import persistence

from pymemcache.client.base import Client


async def metadata_queue_watcher(database: persistence.Database, metadata_queue: asyncio.Queue, node) -> None:
    """
     Watches for the metadata queue to commit any complete info hashes to the database.
    """
    while True:
        info_hash, metadata = await metadata_queue.get()
        # print(info_hash, metadata)
        succeeded = database.add_metadata(info_hash, metadata, node)
        if not succeeded:
            logging.info("Corrupt metadata for %s! Ignoring.", info_hash.hex())


def parse_port(port):
    if ',' in port:
        return map(int, port.split(','))
    if '-' in port:
        a, b = port.split('-')
        return list(range(int(a), int(b) + 1))
    return [int(port)]


def parse_size(value: str) -> int:
    try:
        return humanfriendly.parse_size(value)
    except humanfriendly.InvalidSize as e:
        raise argparse.ArgumentTypeError("Invalid argument. {}".format(e))


def parse_cmdline_arguments(args: typing.List[str]) -> typing.Optional[argparse.Namespace]:
    parser = argparse.ArgumentParser(
        description="Autonomous BitTorrent DHT crawler and metadata fetcher.",
        epilog=textwrap.dedent("""\
            Copyright (C) 2017  Mert Bora ALPER <bora@boramalper.org>
            Dedicated to Cemile Binay, in whose hands I thrived.

            This program is free software: you can redistribute it and/or modify it under
            the terms of the GNU Affero General Public License as published by the Free
            Software Foundation, either version 3 of the License, or (at your option) any
            later version.

            This program is distributed in the hope that it will be useful, but WITHOUT ANY
            WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
            PARTICULAR PURPOSE.  See the GNU Affero General Public License for more
            details.

            You should have received a copy of the GNU Affero General Public License along
            with this program.  If not, see <http://www.gnu.org/licenses/>.
        """),
        allow_abbrev=False,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )


    parser.add_argument(
        '-I', "--host", action="store", required=False, default=os.getenv('NODE_HOST', "0.0.0.0"),
        help="the host of the (DHT) node magneticod will use"
    )

    parser.add_argument(
        '-P', "--port", action="store", type=parse_port, required=False, default=os.getenv('NODE_PORT', "1910"),
        help="the port of the (DHT) node magneticod will use"
    )

    parser.add_argument(
        '-s', "--max-metadata-size", type=parse_size, default=DEFAULT_MAX_METADATA_SIZE,
        help="Limit metadata size to protect memory overflow. Provide in human friendly format eg. 1 M, 1 GB"
    )

    default_database = 'sqlite:///' + os.path.join(appdirs.user_data_dir("magneticod"), "database.sqlite3")
    default_database = os.getenv('DATABASE', default_database)
    parser.add_argument(
        '-D', "--database", type=str, default=default_database,
        help="Database url (default: {}). Extra possible formats: postgresql://user:pass@host:port/dbname".format(default_database)
    )
    parser.add_argument(
        '-d', '--debug',
        action="store_const", dest="loglevel", const=logging.DEBUG, default=logging.INFO,
        help="Print debugging information in addition to normal processing.",
    )
    parser.add_argument(
        '-S', '--stats',
        action="store_true", default=False,
        help="Save stats info to files.",
    )
    parser.add_argument(
        '-M', '--memcache',
        default=None, help="Enable memcached cache.",
    )
    parser.add_argument(
        '-n', '--max-neighbours', default=2000, type=int,
        help="Set max neighbours count.",
    )
    parser.add_argument(
        '-B', '--batch-size', default=1, type=int,
        help="Commit batch size.",
    )
    parser.add_argument(
        '-i', '--stats-interval', default=10, type=int,
        help="Stats interval.",
    )
    parser.add_argument(
        '-H', '--heat-memcache',
        action="store_true", default=False,
        help="Heat memcached and exit.",
    )
    return parser.parse_args(args)


def main() -> int:
    # main_task = create_tasks()
    arguments = parse_cmdline_arguments(sys.argv[1:])

    logging.basicConfig(level=arguments.loglevel, format="%(asctime)s  %(levelname)-8s  %(message)s")
    logging.info("magneticod v%d.%d.%d started", *__version__)

    # use uvloop if it's installed
    # try:
    #     import uvloop
    #     asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    #     logging.info("uvloop is in use")
    # except ImportError:
    #     if sys.platform not in ["linux", "darwin"]:
    #         logging.warning("uvloop could not be imported, using the default asyncio implementation")


    # noinspection PyBroadException
    try:
        database = persistence.Database(
            arguments.database, commit_n=arguments.batch_size
        )
    except:
        logging.exception("could NOT connect to the database!",
                          exc_info=False)
        return 1

    if arguments.heat_memcache:
        cache = Client((
            arguments.memcache.split(':')[0],
            int(arguments.memcache.split(':')[1])
        ))
        database.heat_memcache(cache)
        return


    loop = asyncio.get_event_loop()
    cancel_on_exit = []
    nodes = []
    for port in arguments.port:
        node = dht.SybilNode(
            database.is_infohash_new,
            arguments.max_metadata_size,
            arguments.max_neighbours,
            arguments.memcache,
            debug_path='stats.' + str(port) if arguments.stats else None
        )
        loop.create_task(node.launch((arguments.host, port)))
        # mypy ignored: mypy doesn't know (yet) about coroutines
        metadata_queue_watcher_task = loop.create_task(metadata_queue_watcher(database, node.metadata_q(), node))  # type: ignore
        print_info_task = loop.create_task(database.print_info(node, delay=arguments.stats_interval))  # type: ignore
        reset_counters_task = loop.create_task(database.reset_counters(node, delay=3600))  # type: ignore

        cancel_on_exit.append(metadata_queue_watcher_task)
        cancel_on_exit.append(print_info_task)
        cancel_on_exit.append(reset_counters_task)
        nodes.append(node)


    try:
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        logging.critical("Keyboard interrupt received! Exiting gracefully...")
    finally:
        for task in cancel_on_exit:
            task.cancel()
        for node in nodes:
            loop.run_until_complete(node.shutdown())
        database.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
