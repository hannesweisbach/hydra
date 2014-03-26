#include <iostream>

#include "messages.h"

static std::ostream &operator<<(std::ostream &s, const response &r) {
  s << indent << "uint64_t cookie = " << std::showbase << r.cookie()
    << std::endl;
  s << indent;
  switch (r.subtype()) {
  case msg::subtype::put: {
    const put_response &r_ = static_cast<const put_response &>(r);
    s << std::boolalpha << r_.value();
  } break;
  case msg::subtype::del: {
    const remove_response &r_ = static_cast<const remove_response &>(r);
    s << r_.value();
  } break;
  case msg::subtype::init: {
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
  case msg::subtype::put: {
    const put_request &r = static_cast<const put_request &>(req);
    s << r.key() << r.value();
  } break;
  case msg::subtype::del: {
    const remove_request &r = static_cast<const remove_request &>(req);
    s << r.key();
  } break;
  case msg::subtype::disconnect:
  case msg::subtype::init:
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
  return s;
}

static std::ostream& operator<<(std::ostream & s, const notification_resize & resize) {
  s << indent << "mr init = " << resize.init();
  return s;
}

std::ostream &operator<<(std::ostream &s, const msg &m) {
  s << std::hex;
  s << "msg {" << std::endl;
  {
    indent_guard guard(s);
    s << indent << "enum type type = " << m.type() << std::endl;
    s << indent << "enum subtype subtype = " << m.subtype() << std::endl;
    switch (m.type()) {
    case msg::type::request:
      s << static_cast<const request &>(m);
      break;
    case msg::type::response:
      s << static_cast<const response &>(m);
      break;
    case msg::type::notification:
      switch (m.subtype()) {
      case msg::subtype::resize:
        s << static_cast<const notification_resize &>(m);
        break;
      }
      break;
    case msg::type::invalid:
      break;
    }
  }
  s << std::endl << indent << "};" << std::dec;
  return s;
}

std::ostream &operator<<(std::ostream &s, const mr &mr) {
  s << "mr {" << std::endl;
  {
    indent_guard guard(s);
    s << indent << "uint64_t addr = " << std::hex << std::showbase
      << std::setfill('0') << std::setw(12) << mr.addr << std::dec << std::endl;
    s << indent << "uint32_t size = " << mr.size << std::endl;
    s << indent << "uint32_t rkey = " << mr.rkey << std::endl;
  }
  s << indent << "};";
  return s;
}

void response::complete_() const {
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
  case subtype::init: {
    auto f = reinterpret_cast<std::function<void(const mr&)> *>(cookie);
    auto response = static_cast<const mr_response &>(*this);
    (*f)(response.value());
    delete f;
  } break;
  default:
    assert(false);
  }
}
