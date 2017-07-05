from peewee import Proxy, Model, BlobField, TextField, IntegerField, Check, \
    ForeignKeyField
from playhouse.shortcuts import model_to_dict

database_proxy = Proxy()


class BaseModel(Model):
    class Meta:
        database = database_proxy

    def to_dict(self):
        return model_to_dict(self)

    @staticmethod
    def atomic(*args, **kwargs):
        return database_proxy.atomic(*args, **kwargs)

    @staticmethod
    def commit(*args, **kwargs):
        return database_proxy.commit(*args, **kwargs)

    @staticmethod
    def rollback(*args, **kwargs):
        return database_proxy.rollback(*args, **kwargs)


class Torrent(BaseModel):
    class Meta:
        db_table = 'torrents'

    info_hash = BlobField(unique=True)
    name = TextField()
    total_size = IntegerField(constraints=[Check('total_size > 0')])
    discovered_on = IntegerField(constraints=[Check('discovered_on > 0')])


class File(BaseModel):
    class Meta:
        db_table = 'files'

    torrent = ForeignKeyField(
        Torrent, on_delete='CASCADE', on_update='RESTRICT')
    size = IntegerField()
    path = TextField()
