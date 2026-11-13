import time
from concurrent import futures

import grpc

from miniredis_grpc import kvstore_pb2_grpc
from miniredis_grpc.kv_store_service import KeyValueStoreServicer


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = KeyValueStoreServicer()
    kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(servicer, server)

    server.add_insecure_port("[::]:8000")
    server.start()

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        servicer.db.close()
        server.stop(0)


if __name__ == "__main__":
    serve()
