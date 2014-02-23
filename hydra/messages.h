#pragma once

#include <cstring>
#include <rdma/rdma_verbs.h>

#include "utils.h"
#include "util/Logger.h"
/*
 * message types:
 * add(key, key_len, key_rkey, value, value_len, value_rkey):
 *   where key and value are ptrs in the clients address space and key_len
 *   is the length of the key in bytes and value_len is the length of the
 *   object in bytes. the lengths are inclusive of a possible terminating zero.
 *   key_rkey is the rkey to read the key and value_rkey is the rkey to be used
 *   to read the value. The key and the value must each be contained within a
 *   single mr. They need not be contained in the same mr, although they might
 *   be.
 *   the server rdma-reads the key and value from the client into local memory
 *   (which is already RDMA-read-mapped for all clients). This results in a
 *   zero-copy transfer of data. The server then calculates the hash for the
 *   key and updates its hash table.
 *   If (key + key_len) == value and (key_rkey == value_rkey, then the server
 *   may coalesce the rdma-read of the key and the value into a single
 *   transfer.
 *   TODO: V1: After the data is copied, the server signals to the client, that
 *   the data was transferred (this does not imply, that the (key, value) can
 *   be looked up in the hash table. The insertion may at this point still
 *   fail, for example if the server is out of memory or additional memory
 *   pages cannot be pinned.
 *   TODO: V2 (preferred): After the insertion, the client is informed whether
 *   it was successful or not. the client may at this point unmap its buffers.
 *   In case of an error, the client may perform any recovery operations it
 *   deems neccessary.
 *   This means the client has to contact the correct node on its own, routing
 *   between nodes may still take place, but the data (at least the key) has to
 *   be transferred multiple times.
 * 
 * remove(key, key_len, key_rkey):
 *   where key is a ptr in the clients address space, key_len is the length of
 *   the key in bytes (including a possible terminating zero) and key_rkey is
 *   the rkey for the mr, the key is located in. The key must be contained in
 *   a single mr.
 *   the server reads the key in a local buffer, calculates the hash, table 
 *   index and performs necessary modifications of the hash table.
 *   After the server completed the hash table modifications, it signals the
 *   client the completion of the operation including success or failure.
 *
 * After a failed add or remove operation all (key, value) pairs contained in
 * the hash table before the failed operation will be available for lookup and
 * retrieval after the failed operation, although MRs and rkeys may have
 * changed.
 *
 * node-join():
 * node-leave():
 * routing:
 *
 */

#define init_member_from(member, ptr)                                          \
  do {                                                                         \
    memcpy(&member, ptr, sizeof(member));                                      \
    ptr += sizeof(member);                                                     \
  } while (0)

enum DHT_MSG_TYPE {
  INVALID,
  ADD,
  DEL,
  ACK,
  NACK,
  INIT,
  DISCONNECT,
  RESIZE
};

struct msg_header {
  enum DHT_MSG_TYPE type;
  uint32_t pad;
  uint64_t client_id;
};

struct mr {
  uint64_t addr;
  uint32_t size;
  uint32_t rkey;
  mr() = default;
  mr(ibv_mr *mr) noexcept : addr(reinterpret_cast<uint64_t>(mr->addr)),
                            size(static_cast<uint32_t>(mr->length)),
                            rkey(mr->rkey) {}
  template <typename T>
  mr(const T *const p, size_t size, uint32_t rkey) noexcept
      : addr(reinterpret_cast<uint64_t>(p)),
        size(static_cast<uint32_t>(size)),
        rkey(rkey) {}
};

struct resize_data {
  struct mr infopage;
};

struct init_data {
  struct mr infopage;
};

struct node_init {
  __uint128_t start;
  __uint128_t end;
  mr host;
  uint16_t port;
};

/* key and value pointers are uint64_t, so that 64 and 32 bit nodes are
 * interoperable. I believe the maximum transferrable size is 2GiB, so
 * 32 bit lengths are ok.
 */
struct add_data {
#if 1
  uint64_t cookie;
  struct mr key;
  struct mr value;
#else
  uint64_t key;
  uint64_t value;
  uint64_t key_len;
  uint64_t value_len;
  uint32_t key_rkey;
  uint32_t value_rkey;
#endif
};

struct del_data {
#if 1
  uint64_t cookie;
  struct mr key;
#else
  uint64_t key;
  uint32_t key_len;
  uint32_t key_rkey;
#endif
};

struct ack_data {
  enum DHT_MSG_TYPE acked;
  uint64_t cookie;
  uint64_t key;
  uint32_t key_length;
};

constexpr size_t max_size = sizeof_largest_type<init_data, add_data, del_data, ack_data>() + sizeof(msg_header);

struct dht_msg {
  enum DHT_MSG_TYPE type;
  uint64_t client_id;
  uint8_t data[max_size];
};

//TODO: add volatile
//TODO: use offsetof?
//TODO: use is_standard_layout?
//add ackable_msg type/type trait
//
struct msg {
protected:
  /*volatile */uint8_t _data[max_size];

public:
  msg(enum DHT_MSG_TYPE type = INVALID, uint64_t id = 0xdeadbabe) noexcept {
    memcpy(_data, &type, sizeof(type));
    memcpy(_data + sizeof(type) + sizeof(uint32_t), &id, sizeof(id));
  }
  enum DHT_MSG_TYPE type() const {
    enum DHT_MSG_TYPE type;
    memcpy(&type, _data, sizeof(type));
    return type;
  }
  uint64_t id() const {
    uint64_t id;
    memcpy(&id, _data + sizeof(enum DHT_MSG_TYPE) + sizeof(uint32_t), sizeof(uint64_t));
    return id;
  }
  const uint8_t *data() const { return &_data[0]; }
  size_t size() const { return sizeof(_data); }
  //const uint8_t *payload() const { return &_data[sizeof(msg_header)]; }
};

class msg_add : public msg {
public:
  msg_add(uint64_t id, uint64_t cookie, mr key, mr value) noexcept : msg(ADD, id) {
    memcpy(_data + sizeof(msg_header), &cookie, sizeof(cookie));
    memcpy(_data + sizeof(msg_header) + sizeof(cookie), &key, sizeof(key));
    memcpy(_data + sizeof(msg_header) + sizeof(cookie) + sizeof(key), &value, sizeof(value));
  }
  msg_add(const msg &m) noexcept : msg(m) {}
  uint64_t cookie() const {
    uint64_t tmp;
    memcpy(&tmp, _data + sizeof(msg_header), sizeof(tmp));
    return tmp;
  }
  struct mr key() const {
    struct mr tmp;
    memcpy(&tmp, _data + sizeof(msg_header) + sizeof(uint64_t), sizeof(tmp));
    return tmp;
  }
  struct mr value() const {
    struct mr tmp;
    memcpy(&tmp, _data + sizeof(msg_header) + sizeof(uint64_t) + sizeof(mr), sizeof(tmp));
    return tmp;
  }
};

class msg_del : public msg {
public:
  msg_del(uint64_t id, uint64_t cookie, mr key) noexcept : msg(DEL, id) {
    memcpy(_data + sizeof(msg_header), &cookie, sizeof(cookie));
    memcpy(_data + sizeof(msg_header) + sizeof(cookie), &key, sizeof(key));
  }
  msg_del(const msg &m) noexcept : msg(m) {}
  uint64_t cookie() const {
    uint64_t tmp;
    memcpy(&tmp, _data + sizeof(msg_header), sizeof(tmp));
    return tmp;
  }
  struct mr key() const {
    struct mr tmp;
    memcpy(&tmp, _data + sizeof(msg_header) + sizeof(uint64_t), sizeof(tmp));
    return tmp;
  }
};

class msg_ack : public msg {
public:
  msg_ack(const msg &m) noexcept : msg(m) {}
  //I think this is better.
  msg_ack(const msg_add& m) noexcept : msg_ack(m.type(), m.cookie(), m.key()) {}
  msg_ack(const msg_del& m) noexcept : msg_ack(m.type(), m.cookie(), m.key()) {}
  msg_ack(enum DHT_MSG_TYPE type = INVALID, uint64_t cookie = 0,
          mr key = {}) noexcept : msg(ACK) {
    memcpy(_data + sizeof(msg_header), &type, sizeof(type));
    memcpy(_data + sizeof(msg_header) + sizeof(type), &cookie, sizeof(cookie));
    memcpy(_data + sizeof(msg_header) + sizeof(type) + sizeof(cookie),
           &key.addr, sizeof(key.addr));
    memcpy(_data + sizeof(msg_header) + sizeof(type) + sizeof(cookie) +
               sizeof(key.addr),
           &key.size, sizeof(key.size));
  }
  enum DHT_MSG_TYPE ack_type() const {
    enum DHT_MSG_TYPE type;
    memcpy(&type, _data + sizeof(msg_header), sizeof(type));
    return type;
  }
  uint64_t cookie() const {
    uint64_t tmp;
    memcpy(&tmp, _data + sizeof(msg_header) + sizeof(enum DHT_MSG_TYPE),
           sizeof(tmp));
    return tmp;
  }
  uint64_t addr() const {
    uint64_t tmp;
    memcpy(&tmp, _data + sizeof(msg_header) + sizeof(uint64_t) + sizeof(enum DHT_MSG_TYPE), sizeof(tmp));
    return tmp;
  }
  uint32_t key_size() const {
    uint32_t tmp;
    memcpy(&tmp, _data + sizeof(msg_header) + sizeof(uint64_t) + sizeof(enum DHT_MSG_TYPE) + sizeof(uint64_t), sizeof(tmp));
    return tmp;
  }
};

class msg_init : public msg {
public:
  msg_init(uint64_t id, mr init) noexcept : msg(INIT, id) {
    memcpy(_data + sizeof(msg_header), &init, sizeof(init));
  }
  msg_init(const msg &m) noexcept : msg(m) {}
  mr init() const {
    mr tmp;
    memcpy(&tmp, _data + sizeof(msg_header), sizeof(mr));
    return tmp;
  }
};

class msg_resize : public msg {
public:
  msg_resize(mr init) noexcept : msg(RESIZE) {
    memcpy(_data + sizeof(msg_header), &init, sizeof(init));
  }
  msg_resize(const msg &m) : msg(m) {}
  mr init() const {
    mr tmp;
    memcpy(&tmp, _data + sizeof(msg_header), sizeof(mr));
    return tmp;
  }
};

class msg_disconnect : public msg {
public:
  msg_disconnect(uint64_t id) noexcept : msg(DISCONNECT, id) {}
};

std::ostream &operator<<(std::ostream &s, enum DHT_MSG_TYPE& type);
std::ostream &operator<<(std::ostream &s, const msg &m);
std::ostream &operator<<(std::ostream &s, const msg_add &m);
std::ostream &operator<<(std::ostream &s, const msg_init &m);
std::ostream &operator<<(std::ostream &s, const mr &mr);
std::ostream &operator<<(std::ostream &s, const msg_ack &m);
