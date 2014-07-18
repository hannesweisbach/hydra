#include <iostream>

#include "messages.h"

static std::ostream &operator<<(std::ostream &s, const response &r) {
  s << indent << "uint64_t cookie = " << std::showbase << r.cookie()
    << std::endl;
  s << indent;
  switch (r.subtype()) {
  case msg::msubtype::put: {
    const put_response &r_ = static_cast<const put_response &>(r);
    s << std::boolalpha << r_.value();
  } break;
  case msg::msubtype::del: {
    const remove_response &r_ = static_cast<const remove_response &>(r);
    s << r_.value();
  } break;
  case msg::msubtype::init: {
    auto response = static_cast<const init_response &>(r);
    s << response.value();
  } break;
  default:
    assert(false);
    break;
  }
  return s;
}

static std::ostream &operator<<(std::ostream &s, const request &req) {
  s << indent << "uint64_t cookie = " << std::showbase << req.cookie()
    << std::endl;
  s << indent;
  switch (req.subtype()) {
  case msg::msubtype::put: {
    const put_request &r = static_cast<const put_request &>(req);
    s << r.key() << r.value();
  } break;
  case msg::msubtype::del: {
    const remove_request &r = static_cast<const remove_request &>(req);
    s << r.key();
  } break;
  case msg::msubtype::init:
    break;
  default:
    assert(false);
    break;
  }
  return s;
}

static std::ostream &operator<<(std::ostream &s, const msg::mtype &type) {
  switch (type) {
  case msg::mtype::invalid:
    return s << "invalid";
  case msg::mtype::request:
    return s << "request";
  case msg::mtype::response:
    return s << "response";
  case msg::mtype::notification:
    return s << "notification";
  }
  return s;
}

static std::ostream &operator<<(std::ostream &s,
                                const msg::msubtype subtype) {
  switch (subtype) {
  case msg::msubtype::invalid:
    return s << "invalid";
  case msg::msubtype::put:
    return s << "put";
  case msg::msubtype::del:
    return s << "del";
  case msg::msubtype::init:
    return s << "init";
  case msg::msubtype::resize:
    return s << "resize";
  case msg::msubtype::predecessor:
    return s << "predecessor";
  case msg::msubtype::routing:
    return s << "routing";
  }
  return s;
}

static std::ostream &operator<<(std::ostream &s,
                                const notification_resize &resize) {
  s << indent << "mr init = " << resize.init();
  return s;
}

static std::ostream &operator<<(std::ostream &s, const notification_update &n) {
  s << indent << "node_id = " << n.node() << std::endl;
  return s << indent << "index = " << n.index();
}

static std::ostream &operator<<(std::ostream &s,
                                const notification_predecessor &p) {
  return s << indent << "node_id = " << p.predecessor();
}

std::ostream &operator<<(std::ostream &s, const msg &m) {
  s << std::hex;
  s << "msg {" << std::endl;
  {
    indent_guard guard(s);
    s << indent << "enum type type = " << m.type() << std::endl;
    s << indent << "enum subtype subtype = " << m.subtype() << std::endl;
    switch (m.type()) {
    case msg::mtype::request:
      s << static_cast<const request &>(m);
      break;
    case msg::mtype::response:
      s << static_cast<const response &>(m);
      break;
    case msg::mtype::notification:
      switch (m.subtype()) {
      case msg::msubtype::resize:
        s << static_cast<const notification_resize &>(m);
        break;
      case msg::msubtype::predecessor:
        s << static_cast<const notification_predecessor &>(m);
        break;
      case msg::msubtype::routing:
        s << static_cast<const notification_update &>(m);
        break;
      }
      break;
    case msg::mtype::invalid:
      break;
    }
  }
  s << std::endl << indent << "};" << std::dec;
  return s;
}

void response::complete_() const {
  uintptr_t cookie;
  memcpy(&cookie, data_ + offsetof(header, cookie), sizeof(uint64_t));

  assert(type() == mtype::response);

  switch (subtype()) {
  case msubtype::put:
  case msubtype::del: {
    bool ack;
    auto f = reinterpret_cast<std::function<void(bool)> *>(cookie);
    memcpy(&ack, data_ + sizeof(header), sizeof(ack));
    (*f)(ack);
    delete f;
  } break;
  case msubtype::init: {
    auto f = reinterpret_cast<std::function<void(const mr&)> *>(cookie);
    auto response = static_cast<const mr_response &>(*this);
    (*f)(response.value());
    delete f;
  } break;
  default:
    assert(false);
  }
}
