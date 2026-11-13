import time
from concurrent import futures

import grpc

from miniredis_grpc import kvstore_pb2, kvstore_pb2_grpc


class KeyValueStoreServicer(kvstore_pb2_grpc.KeyValueStoreServicer):
    def __init__(self):
        # Здесь будет ваша логика хранения данных, TTL и LRU
        self.storage = {}

    def Put(self, request, context):
        print(
            f"Put: key={request.key}, value={request.value}, ttl={request.ttl_seconds}"
        )
        # TODO: Реализовать сохранение с учетом TTL
        self.storage[request.key] = request.value
        return kvstore_pb2.PutResponse()

    def Get(self, request, context):
        print(f"Get: key={request.key}")
        value = self.storage.get(request.key)

        if value is None:
            # Возвращаем gRPC-ошибку NOT_FOUND, если ключ отсутствует или TTL истёк
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Key not found or expired")
            return kvstore_pb2.GetResponse()

        return kvstore_pb2.GetResponse(value=value)

    def Delete(self, request, context):
        print(f"Delete: key={request.key}")
        if request.key in self.storage:
            del self.storage[request.key]
        return kvstore_pb2.DeleteResponse()

    def List(self, request, context):
        print(f"List: prefix={request.prefix}")
        items = []

        for k, v in self.storage.items():
            if k.startswith(request.prefix):
                items.append(kvstore_pb2.KeyValue(key=k, value=v))

        return kvstore_pb2.ListResponse(items=items)


def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(
        KeyValueStoreServicer(), server
    )

    server.add_insecure_port("[::]:8000")
    server.start()
    print("gRPC сервер KeyValueStore запущен на порту 8000...")

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    server()
