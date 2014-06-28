@0x9f917e549d92b283;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("hydra::protocol");

struct Mr {
  addr @0 :UInt64;
  size @1 :UInt32;
  rkey @2 :UInt32;
}

struct DHTMessage {
  union {
    put :group {
      union {
        remote :group {
          mr @0 :Mr;
        }
        inline :group {
          keySize @1 :UInt8;
          size @2 :UInt8;
          data @3 :Data;
        }
      }
    }
    del :group {
      union {
        remote :group {
          mr @4 :Mr;
        }
        inline :group {
          size @5 :UInt8;
          key  @6 :Data;
        }
      }
    }
    ack :group {
      success @7 :Bool;
    }
  }
}
