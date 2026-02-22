import copy
import json
import hashlib
from typing import Any, Dict, Optional

class VersionedKVStore:
    def __init__(self):
        self._version = 0
        self._state = {}
        self._snapshots = {0: {}}

    @staticmethod
    def _canonical(obj: Dict[str, Any]) -> str:
        return json.dumps(obj, sort_keys=True, separators=(',', ':'))

    @staticmethod
    def _digest_for(obj: Dict[str, Any]) -> str:
        s = VersionedKVStore._canonical(obj).encode()
        return hashlib.sha256(s).hexdigest()

    def version(self) -> int:
        return self._version

    def snapshot(self, version: Optional[int] = None) -> Dict[str, Any]:
        v = self._version if version is None else version
        snap = self._snapshots.get(v)
        if snap is None:
            raise ValueError(f"Unknown version {v}")
        return copy.deepcopy(snap)

    def digest(self, version: Optional[int] = None) -> str:
        snap = self.snapshot(version)
        return self._digest_for(snap)

    def apply(self, op: Dict[str, Any]) -> int:
        st = copy.deepcopy(self._state)
        t = op.get("type")
        k = op.get("key")
        if t == "SET":
            st[k] = op.get("value")
        elif t == "INCR":
            val = int(st.get(k, 0))
            st[k] = val + int(op.get("delta", 1))
        elif t == "APPEND":
            lst = list(st.get(k, []))
            lst.append(op.get("value"))
            st[k] = lst
        elif t == "DELETE":
            st.pop(k, None)
        else:
            raise ValueError(f"Unknown op type: {t}")
        self._state = st
        self._version += 1
        self._snapshots[self._version] = copy.deepcopy(self._state)
        return self._version

    def get_version(self, version: Optional[int] = None, *args, **kwargs) -> Dict[str, Any]:
        v = self._version if version is None else version
        snap = self._snapshots.get(v, self._state)
        return copy.deepcopy(snap)

    def commit_version(self, *args, **kwargs) -> int:
        if args and isinstance(args[0], int):
            self._version = args[0]
        else:
            self._version += 1
            
        self._snapshots[self._version] = copy.deepcopy(self._state)
        return self._version

    def __setitem__(self, key, value):
        self._state[key] = value

    def __getitem__(self, key):
        return self._state[key]

    def get(self, key, default=None):
        return self._state.get(key, default)

    def values(self):
        return self._state.values()

    def keys(self):
        return self._state.keys()

    def items(self):
        return self._state.items()
