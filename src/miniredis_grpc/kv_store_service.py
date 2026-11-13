import grpc

from miniredis_grpc import kvstore_pb2, kvstore_pb2_grpc
from miniredis_grpc.miniredis import MiniRedis


class KeyValueStoreServicer(kvstore_pb2_grpc.KeyValueStoreServicer):
    def __init__(self):
        self.db = MiniRedis()

    def Put(self, request, context):
        self.db.put(request.key, request.value, request.ttl_seconds)
        return kvstore_pb2.PutResponse()

    def Get(self, request, context):
        value = self.db.get(request.key)

        if value is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Key not found or expired")
            return kvstore_pb2.GetResponse()

        return kvstore_pb2.GetResponse(value=value)

    def Delete(self, request, context):
        self.db.delete(request.key)
        return kvstore_pb2.DeleteResponse()

    def List(self, request, context):
        pairs = self.db.list_by_prefix(request.prefix)
        items = [kvstore_pb2.KeyValue(key=k, value=v) for k, v in pairs]
        return kvstore_pb2.ListResponse(items=items)
