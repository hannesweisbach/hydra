@0x9f917e549d92b283;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("hydra::protocol");

struct Mr {
  addr @0 :UInt64;
  size @1 :UInt32;
  rkey @2 :UInt32;
}

struct DHTRequest {
  union {
    put :group {
      union {
        remote :group {
          key @0 :Mr;
          value @1 :Mr;
        }
        inline :group {
          keySize @2 :UInt8;
          size @3 :UInt8;
          data @4 :Data;
        }
      }
    }
    del :group {
      union {
        remote :group {
          mr @5 :Mr;
        }
        inline :group {
          size @6 :UInt8;
          key  @7 :Data;
        }
      }
    }
  }
}

struct DHTResponse {
  ack :group {
    success @0 :Bool;
  }
}
