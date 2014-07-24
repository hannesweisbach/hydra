@0x9c34900a20422afd;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("hydra::protocol");

struct Node {
  ip @0 :Text;
  port @1 :Text;
  id @2 :Data;
}

struct Mr {
  addr @0 :UInt64;
  size @1 :UInt32;
  rkey @2 :UInt32;
}

struct DHTRequest {
  union {
    put :union {
      remote :group {
        kv @0 :Mr;
        keySize @1 :UInt32;
      }
      inline :group {
        keySize @2 :UInt8;
        size @3 :UInt8;
        data @4 :Data;
      }
    }
    del :union {
      remote :group {
        key @5 :Mr;
      }
      inline :group {
        size @6 :UInt8;
        key  @7 :Data;
      }
    }
    init @8 :Void;
    predecessor : group {
      node @9 :Node;
    }
    update :group {
      node @10 :Node;
      index @11 :UInt64;
    }
    network @12 :Void;
  }
}

struct DHTResponse {
  enum NetworkType {
    fixed @0;
    chord @1;
  }
  union {
    ack :group {
      success @0 :Bool;
    }
    init :group {
      info @1 :Mr;
    }
    network : group {
      type @2 :NetworkType;
      table @3 :Mr;
      size @4 :UInt16;
    }
  }
}
