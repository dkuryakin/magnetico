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
import logging
import sqlite3
import time
import typing
import os
import peewee
from .models import Torrent, File, database_proxy
from playhouse.db_url import connect, parse
from magneticod import bencode
from collections import Counter
import asyncio


class Database:
    def __init__(self, database, commit_n=10) -> None:
        self._commit_n = commit_n
        kw = {}
        self._cnt = Counter()
        if database.startswith('sqlite://'):
            kw['pragmas'] = [
                ('journal_mode', 'WAL'),
                ('temp_store', '1'),
                ('foreign_keys', 'ON')
            ]
        self._db = database
        self._kw = kw
        self._connect()

        # We buffer metadata to flush many entries at once, for performance reasons.
        # list of tuple (info_hash, name, total_size, discovered_on)
        self.__pending_metadata = []  # type: typing.List[typing.Dict]
        # list of tuple (info_hash, size, path)
        self.__pending_files = []  # type: typing.List[typing.Dict]

    def _connect(self):
        db = connect(self._db, **self._kw)
        database_proxy.initialize(db)
        database_proxy.create_tables([Torrent, File], safe=True)

    async def print_info(self, node, delay=3600):
        while True:
            try:
                logging.info('STATS nodes:%d/%d catched_hash:%d known_hash:%d %.2f%% added_hash:%d %.2f%% bd_errors:%d lcache:%d/%d max:%d',
                    node._cnt['nodes'],
                    node._skip,
                    self._cnt['catched'],
                    self._cnt['known'],
                    self._cnt['known'] * 100 / self._cnt['catched'] if self._cnt['catched'] else 0,
                    self._cnt['added'],
                    self._cnt['added'] * 100 / self._cnt['catched'] if self._cnt['catched'] else 0,
                    self._cnt['errors'],
                    len(node._hashes),
                    node._collisions,
                    node._n_max_neighbours,
                )
            except:
                pass
            node._cnt = Counter()
            self._cnt = Counter()
            await asyncio.sleep(delay)

    def add_metadata(self, info_hash: bytes, metadata: bytes, node) -> bool:
        files = []
        discovered_on = int(time.time())
        try:
            if metadata == b'test':
                info = {b'name': b'test', b'length': 123}
            else:
                info = bencode.loads(metadata)

            assert b"/" not in info[b"name"]
            name = info[b"name"].decode("utf-8")

            if b"files" in info:  # Multiple File torrent:
                for file in info[b"files"]:
                    assert type(file[b"length"]) is int
                    # Refuse trailing slash in any of the path items
                    assert not any(b"/" in item for item in file[b"path"])
                    path = "/".join(i.decode("utf-8") for i in file[b"path"])
                    subq = Torrent.select(Torrent.id).where(
                        Torrent.info_hash == info_hash)
                    files.append({
                        'torrent': subq,
                        'size': file[b"length"],
                        'path': path
                    })
            else:  # Single File torrent:
                assert type(info[b"length"]) is int
                subq = Torrent.select(Torrent.id).where(
                    Torrent.info_hash == info_hash)
                files.append({
                    'torrent': subq,
                    'size': info[b"length"],
                    'path': name
                })
        except peewee.InterfaceError:
            self._connect()
            return False
        except (
                bencode.BencodeDecodingError, AssertionError, KeyError,
                AttributeError,
                UnicodeDecodeError, TypeError):
            logging.exception('Not critical error.', exc_info=False)
            return False

        self.__pending_metadata.append({
            'info_hash': info_hash,
            'name': name,
            'total_size': sum(f['size'] for f in files),
            'discovered_on': discovered_on
        })
        # MYPY BUG: error: Argument 1 to "__iadd__" of "list" has incompatible type List[Tuple[bytes, Any, str]];
        #     expected Iterable[Tuple[bytes, int, bytes]]
        # List is an Iterable man...
        self.__pending_files += files  # type: ignore

        logging.info("Added: `%s`", name)

        # Automatically check if the buffer is full, and commit to the SQLite database if so.
        if len(self.__pending_metadata) >= self._commit_n:
            self.__commit_metadata(node)

        return True

    def is_infohash_new(self, info_hash, skip_check=False):
        try:
            self._cnt['catched'] += 1
            if skip_check:
                return
            if info_hash in [x['info_hash'] for x in self.__pending_metadata]:
                self._cnt['known'] += 1
                return False
            x = Torrent.select().where(Torrent.info_hash == info_hash).count()
            self._cnt['known'] += int(x > 0)
            return x == 0
        except peewee.InterfaceError:
            self._connect()
            raise

    def __commit_metadata(self, node) -> None:
        # noinspection PyBroadException
        n = len(self.__pending_metadata)
        try:
            with database_proxy.atomic():
                Torrent.insert_many(self.__pending_metadata).execute()
                File.insert_many(self.__pending_files).execute()
                self._cnt['added'] += n
                logging.info(
                    "%d metadata (%d files) are committed to the database.",
                    len(self.__pending_metadata), len(self.__pending_files)
                )
                self.__pending_metadata.clear()
                self.__pending_files.clear()
        except peewee.IntegrityError:
            # Some collisions. Drop entire batch to avoid infinite loop.
            # TODO: find better solution
            logging.exception(
                "Could NOT commit metadata to the database because of collisions! (%d metadata were dropped)",
                len(self.__pending_metadata), exc_info=False)
            self.__pending_metadata.clear()
            self.__pending_files.clear()
            self._cnt['errors'] += n
        except peewee.InterfaceError:
            self._connect()
        except:
            logging.exception(
                "Could NOT commit metadata to the database! (%d metadata are pending)",
                len(self.__pending_metadata), exc_info=False)
            self.__pending_metadata.clear()
            self.__pending_files.clear()
            self._cnt['errors'] += n

    def close(self, node) -> None:
        if self.__pending_metadata:
            self.__commit_metadata(node)
