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


def parse_ip_port(netloc: str) -> typing.Optional[typing.Tuple[str, int]]:
    # In case no port supplied
    try:
        return str(ipaddress.ip_address(netloc)), 0
    except ValueError:
        pass

    # If only port was specified
    if netloc.isdigit():
        return '0.0.0.0', int(netloc)

    # In case port supplied
    try:
        parsed = urllib.parse.urlparse("//{}".format(netloc))
        ip = str(ipaddress.ip_address(parsed.hostname))
        port = parsed.port
        if port is None:
            return None
    except ValueError:
        return None

    return ip, port


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
        '-a', "--node-addr", action="store", type=parse_ip_port, required=False, default=os.getenv('NODE_ADDR', "0.0.0.0:1910"),
        help="the address of the (DHT) node magneticod will use"
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
    return parser.parse_args(args)


def main() -> int:
    # main_task = create_tasks()
    arguments = parse_cmdline_arguments(sys.argv[1:])

    logging.basicConfig(level=arguments.loglevel, format="%(asctime)s  %(levelname)-8s  %(message)s")
    logging.info("magneticod v%d.%d.%d started", *__version__)

    # use uvloop if it's installed
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logging.info("uvloop is in use")
    except ImportError:
        if sys.platform not in ["linux", "darwin"]:
            logging.warning("uvloop could not be imported, using the default asyncio implementation")

    # noinspection PyBroadException
    try:
        database = persistence.Database(arguments.database)
    except:
        logging.exception("could NOT connect to the database!", exc_info=False)
        return 1

    loop = asyncio.get_event_loop()
    node = dht.SybilNode(database.is_infohash_new, arguments.max_metadata_size)
    loop.create_task(node.launch(arguments.node_addr))
    # mypy ignored: mypy doesn't know (yet) about coroutines
    metadata_queue_watcher_task = loop.create_task(metadata_queue_watcher(database, node.metadata_q(), node))  # type: ignore
    print_info_task = loop.create_task(database.print_info(node, delay=3600))  # type: ignore

    try:
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        logging.critical("Keyboard interrupt received! Exiting gracefully...")
    finally:
        metadata_queue_watcher_task.cancel()
        print_info_task.cancel()
        loop.run_until_complete(node.shutdown())
        database.close(node)

    return 0


if __name__ == "__main__":
    sys.exit(main())
