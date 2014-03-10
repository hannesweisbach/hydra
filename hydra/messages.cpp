#include <iostream>

#include "messages.h"

static std::ostream &operator<<(std::ostream &s, const response &r) {
  // s << "  uint64_t cookie = " << std::showbase << r.cookie() << std::endl;
  s << "  uint64_t id = " << std::showbase << r.id() << std::endl;
  switch (r.subtype()) {
  case msg::subtype::put: {
    const put_response &r_ = static_cast<const put_response &>(r);
    s << std::boolalpha << r_.value();
  } break;
  case msg::subtype::del: {
    const remove_response &r_ = static_cast<const remove_response &>(r);
    s << r_.value();
  } break;
  default:
    assert(false);
    break;
  }
  return s;
}

static std::ostream &operator<<(std::ostream &s, const request &req) {
  s << "  uint64_t cookie = " << std::showbase << req.cookie() << std::endl;
  s << "  uint64_t id = " << std::showbase << req.id() << std::endl;
  switch (req.subtype()) {
  case msg::subtype::put: {
    const put_request &r = static_cast<const put_request &>(req);
    s << r.key() << r.value();
  } break;
  case msg::subtype::del: {
    const remove_request &r = static_cast<const remove_request &>(req);
    s << r.key();
  } break;
  case msg::subtype::disconnect:
    break;
  default:
    assert(false);
    break;
  }
  return s;
}

static std::ostream &operator<<(std::ostream &s, const enum msg::type &type) {
  switch (type) {
  case msg::type::invalid:
    return s << "invalid";
  case msg::type::request:
    return s << "request";
  case msg::type::response:
    return s << "response";
  case msg::type::notification:
    return s << "notification";
  }
}

static std::ostream &operator<<(std::ostream &s,
                                const enum msg::subtype subtype) {
  switch (subtype) {
  case msg::subtype::invalid:
    return s << "invalid";
  case msg::subtype::put:
    return s << "put";
  case msg::subtype::del:
    return s << "del";
  case msg::subtype::disconnect:
    return s << "disconnect";
  case msg::subtype::init:
    return s << "init";
  case msg::subtype::resize:
    return s << "resize";
  }
}

static std::ostream &operator<<(std::ostream &s, const notification_init &init) {
  s << "  uint64_t id = " << init.id();
  s << "  mr init = " << init.init();
  return s;
}

static std::ostream& operator<<(std::ostream & s, const notification_resize & resize) {
  s << "  mr init = " << resize.init();
  return s;
}

std::ostream &operator<<(std::ostream &s, const msg &m) {
  s << std::hex;
  s << "request {" << std::endl;
  s << "  enum type type = " << m.type() << std::endl;
  s << "  enum subtype subtype = " << m.subtype() << std::endl;
  switch (m.type()) {
  case msg::type::request:
    s << static_cast<const request &>(m);
    break;
  case msg::type::response:
    s << static_cast<const response &>(m);
    break;
  case msg::type::notification:
    switch (m.subtype()) {
    case msg::subtype::init:
      s << static_cast<const notification_init&>(m);
      break;
    case msg::subtype::resize:
      s << static_cast<const notification_resize&>(m);
      break;
    }
    break;
  case msg::type::invalid:
    break;
  }
  s << "};" << std::dec;
  return s;
}

std::ostream& operator<<(std::ostream& s, const mr& mr) {
  s << "mr {" << std::endl;
  s << "  uint64_t addr = " << std::hex << std::showbase << std::setfill('0')
    << std::setw(12) << mr.addr << std::dec << std::endl;
  s << "  uint32_t size = " << mr.size << std::endl;
  s << "  uint32_t rkey = " << mr.rkey << std::endl;
  s << "};";
  return s;
}

