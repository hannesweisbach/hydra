#pragma once

#include <cstring>
#include <memory>
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
  uint64_t cookie;
  uint64_t key;
  uint32_t key_length;
};

class msg {
public:
  enum class type {
    invalid,
    request,
    response,
    notification
  };

  enum class subtype {
    invalid,
    put,
    del,
    disconnect,
    init,
    resize
  };

  struct header {
    enum type type;
    enum subtype subtype;
    uint64_t id;
    uint64_t cookie;
  };

protected:
  static constexpr size_t max_size =
      sizeof_largest_type<init_data, add_data, del_data, ack_data>() +
      sizeof(header);

  uint8_t data_[max_size];

public:
  msg(enum type type = type::invalid,
      enum subtype subtype = subtype::invalid) noexcept {
    memcpy(data_ + offsetof(header, type), &type, sizeof(type));
    memcpy(data_ + offsetof(header, subtype), &subtype, sizeof(subtype));
  }

  enum subtype subtype() const {
    enum subtype stype;
    memcpy(&stype, data_ + offsetof(header, subtype), sizeof(stype));
    return stype;
  }

  enum type type() const {
    enum type type;
    memcpy(&type, data_ + offsetof(header, type), sizeof(type));
    return type;
  }

  // TODO: add void * payload() {...}

  bool is_request() const { return type() == type::request; }

  bool is_response() const { return type() == type::response; }

  bool is_notification() const { return type() == type::notification; }

  bool is_valid() const { return type() != type::invalid; }
};

struct rdma_cm_id;

class request : public msg {
  friend class response;

public:
  request(const rdma_cm_id *id, const enum subtype r,
          const uint64_t cookie = reinterpret_cast<uintptr_t>(nullptr))
      : msg(type::request, r) {
    uint64_t id_ = reinterpret_cast<uintptr_t>(id);
    memcpy(data_ + offsetof(header, id), &id_, sizeof(id_));
    memcpy(data_ + offsetof(header, cookie), &cookie, sizeof(cookie));
  }

  template <typename T, typename F> std::future<T> set_completion(F &&f) {
    auto promise = std::make_shared<std::promise<T> >();
    uint64_t cookie = reinterpret_cast<uintptr_t>(new std::function<void(T)>(
        [ =, functor = std::move(f) ](T result) {
                                       // TODO: maybe make functor void(void)
                                       functor(result);
                                       promise->set_value(result);
                                     }));
    memcpy(data_ + offsetof(header, cookie), &cookie, sizeof(cookie));
    return promise->get_future();
  }

  template <typename F> std::future<void> set_completion(F &&f) {
    auto promise = std::make_shared<std::promise<void> >();
    uint64_t cookie = reinterpret_cast<uintptr_t>(new std::function<void()>([
      =,
      functor = std::move(f)
    ]() {
       functor();
       promise->set_value();
     }));
    memcpy(data_ + offsetof(header, cookie), &cookie, sizeof(cookie));
    return promise->get_future();
  }

  std::future<void> set_completion() {
    auto promise = std::make_shared<std::promise<void> >();
    uint64_t cookie = reinterpret_cast<uintptr_t>(
        new std::function<void()>([=]() { promise->set_value(); }));
    memcpy(data_ + offsetof(header, cookie), &cookie, sizeof(cookie));
    return promise->get_future();
  }

  rdma_cm_id *id() const {
    uint64_t id;
    memcpy(&id, data_ + offsetof(header, id), sizeof(id));
    return reinterpret_cast<rdma_cm_id *>(id);
  }

  uint64_t cookie() const {
    uint64_t cookie;
    memcpy(&cookie, data_ + offsetof(header, cookie), sizeof(cookie));
    return cookie;
  }
};

class put_request : public request {
public:
  put_request(const rdma_cm_id *id, const mr key, const mr value)
      : request(id, subtype::put) {
    memcpy(data_ + sizeof(header), &key, sizeof(key));
    memcpy(data_ + sizeof(header) + sizeof(key), &value, sizeof(value));
  }

  struct mr key() const {
    struct mr tmp;
    memcpy(&tmp, data_ + sizeof(header), sizeof(tmp));
    return tmp;
  }

  struct mr value() const {
    struct mr tmp;
    memcpy(&tmp, data_ + sizeof(header) + sizeof(mr), sizeof(tmp));
    return tmp;
  }
};

class remove_request : public request {
public:
  remove_request(const rdma_cm_id *id, const mr key)
      : request(id, subtype::del) {
    memcpy(data_ + sizeof(header), &key, sizeof(key));
  }

  struct mr key() const {
    struct mr tmp;
    memcpy(&tmp, data_ + sizeof(header), sizeof(tmp));
    return tmp;
  }
};

class disconnect_request : public request {
public:
  disconnect_request(const rdma_cm_id *id) : request(id, subtype::disconnect) {}
};

class response : public msg {
  template <typename T> void complete(T result) {
    std::function<void(T)> *cookie;
    memcpy(reinterpret_cast<void *>(&cookie), data_ + offsetof(header, cookie),
           sizeof(uint64_t));
    (*cookie)(result);
    delete cookie;
  }

  void complete() {
    std::function<void()> *cookie;
    memcpy(reinterpret_cast<void *>(&cookie), data_ + offsetof(header, cookie),
           sizeof(uint64_t));
    (*cookie)();
    delete cookie;
  }

public:
  response(const request &request) : msg(type::response, request.subtype()) {
    uint64_t id = reinterpret_cast<uintptr_t>(request.id());
    auto cookie = request.cookie();
    memcpy(data_ + offsetof(header, id), &id, sizeof(id));
    memcpy(data_ + offsetof(header, cookie), &cookie, sizeof(cookie));
  }

  rdma_cm_id *id() const {
    uint64_t id;
    memcpy(&id, data_ + offsetof(header, id), sizeof(id));
    return reinterpret_cast<rdma_cm_id *>(id);
  }

  void complete_() const {
    uintptr_t cookie;
    memcpy(&cookie, data_ + offsetof(header, cookie), sizeof(uint64_t));

    assert(type() == type::response);

    switch (subtype()) {
    case subtype::put:
    case subtype::del: {
      bool ack;
      auto f = reinterpret_cast<std::function<void(bool)> *>(cookie);
      memcpy(&ack, data_ + sizeof(header), sizeof(ack));
      (*f)(ack);
      delete f;
    } break;
    default:
      assert(false);
    }
  }
};

class bool_response : public response {
public:
  bool_response(const request &request, const bool ack) : response(request) {
    memcpy(data_ + sizeof(header), &ack, sizeof(ack));
  }
  bool value() const {
    bool ack;
    memcpy(&ack, data_ + sizeof(header), sizeof(ack));
    return ack;
  }
};

class put_response : public bool_response {
public:
  put_response(const request &request, bool ack)
      : bool_response(std::move(request), std::move(ack)) {}
};

class remove_response : public bool_response {
public:
  remove_response(const request &request, const bool ack)
      : bool_response(std::move(request), std::move(ack)) {}
};

class notification_init : public msg {
public:
  notification_init(const rdma_cm_id *id, const mr init) noexcept
      : msg(type::notification, subtype::init) {
    memcpy(data_ + offsetof(header, id), &id, sizeof(id));
    memcpy(data_ + sizeof(header), &init, sizeof(init));
  }

  rdma_cm_id *id() const {
    uint64_t id;
    memcpy(&id, data_ + offsetof(header, id), sizeof(id));
    return reinterpret_cast<rdma_cm_id *>(id);
  }

  mr init() const {
    mr tmp;
    memcpy(&tmp, data_ + sizeof(header), sizeof(mr));
    return tmp;
  }
};

class notification_resize : public msg {
public:
  notification_resize(const mr init)
      : msg(type::notification, subtype::resize) {
    memcpy(data_ + sizeof(header), &init, sizeof(init));
  }

  mr init() const {
    mr tmp;
    memcpy(&tmp, data_ + sizeof(header), sizeof(mr));
    return tmp;
  }
};

std::ostream &operator<<(std::ostream &s, const msg &m);
std::ostream &operator<<(std::ostream &s, const mr &mr);