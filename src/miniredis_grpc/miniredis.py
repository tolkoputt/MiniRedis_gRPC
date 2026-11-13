import threading
import time
from collections import OrderedDict


class MiniRedis:
    def __init__(self, max_size=10, cleanup_interval=1.0):
        self.max_size = max_size
        self.store = OrderedDict()
        self.lock = threading.Lock()

        self._stop_event = threading.Event()
        self._cleanup_thread = threading.Thread(
            target=self._background_cleanup,
            args=(cleanup_interval,),
            daemon=True,
        )
        self._cleanup_thread.start()

    def _background_cleanup(self, interval):
        while not self._stop_event.is_set():
            if self._stop_event.wait(interval):
                break

            now = time.time()
            with self.lock:
                expired_keys = [
                    k
                    for k, v in self.store.items()
                    if v["expire_at"] is not None and now > v["expire_at"]
                ]
                for k in expired_keys:
                    self.store.pop(k, None)

    def put(self, key, value, ttl_seconds=0):
        expire_at = time.time() + ttl_seconds if ttl_seconds > 0 else None

        with self.lock:
            if key in self.store:
                self.store.pop(key)
            elif len(self.store) >= self.max_size:
                self.store.popitem(last=False)

            self.store[key] = {"value": value, "expire_at": expire_at}

    def get(self, key):
        with self.lock:
            if key not in self.store:
                return None

            item = self.store[key]

            if item["expire_at"] and time.time() > item["expire_at"]:
                self.store.pop(key)
                return None

            self.store.move_to_end(key)
            return item["value"]

    def delete(self, key):
        with self.lock:
            return self.store.pop(key, None) is not None

    def list_by_prefix(self, prefix):
        now = time.time()
        with self.lock:
            result = []
            for k, v in list(self.store.items()):
                if v["expire_at"] and now > v["expire_at"]:
                    self.store.pop(k, None)
                    continue

                if k.startswith(prefix):
                    result.append((k, v["value"]))
            return result

    def close(self):
        self._stop_event.set()
        self._cleanup_thread.join()
