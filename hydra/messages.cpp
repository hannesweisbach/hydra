#include <iostream>

#include "messages.h"

std::ostream &operator<<(std::ostream &s, enum DHT_MSG_TYPE& type) {
  switch (type) {
  case INVALID:
    s << "INVALID";
    break;
  case ADD:
    s << "ADD";
    break;
  case DEL:
    s << "DEL";
    break;
  case ACK:
    s << "ACK";
    break;
  case NACK:
    s << "NACK";
    break;
  case INIT:
    s << "INIT";
    break;
  case DISCONNECT:
    s << "DISCONNECT";
    break;
  case RESIZE:
    s << "RESIZE";
    break;
  default:
    s << "unkown";
    break;
  }
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

std::ostream &operator<<(std::ostream &s, const msg_ack &m) {
  s << "msg_ack {" << std::endl;
  s << "  enum DHT_MSG_TYPE acked = " << m.ack_type() << std::endl;
  s << "  uint64_t cookie = " << reinterpret_cast<void *>(m.cookie())
    << std::endl;
  s << "  uint64_t key = " << m.addr() << std::endl;
  s << "  uint64_t size = " << m.size() << std::endl;
  s << "};";

  return s;
}

std::ostream &operator<<(std::ostream &s, const msg_add &m) {
  s << "msg_add {" << std::endl;
  s << "  enum DHT_MSG_TYPE type = " << m.type() << std::endl;
  s << "  uint64_t id = " << m.id() << std::endl;
  s << "  uint64_t cookie = " << reinterpret_cast<void *>(m.cookie())
    << std::endl;
  s << "  struct mr key = " << m.key() << std::endl;
  s << "  struct mr value = " << m.value() << std::endl;
  s << "};";
  return s;
}

std::ostream &operator<<(std::ostream &s, const msg_del &m) {
  s << "msg_del {" << std::endl;
  s << "  enum DHT_MSG_TYPE type = " << m.type() << std::endl;
  s << "  uint64_t id = " << m.id() << std::endl;
  s << "  uint64_t cookie = " << reinterpret_cast<void *>(m.cookie())
    << std::endl;
  s << "  struct mr key = " << m.key() << std::endl;
  s << "};";
  return s;
}

std::ostream& operator<<(std::ostream& s, const msg_init& m) {
  return s << m.init();
}

std::ostream& operator<<(std::ostream& s, const msg& m) {
  s << "message {" << std::endl;
  s << "  enum DHT_MSG_TYPE type = " << m.type() << std::endl;
  s << "  uint64_t client_id = " << std::showbase << std::hex << m.id()
    << std::dec << std::endl;
  switch(m.type()) {
    case ADD: s  << msg_add(m); break;
    case INIT: s << msg_init(m); break;
    case ACK: s  << msg_ack(m); break;
    case DEL: s  << msg_del(m); break;
    default: break;
  }
  s << "};";
  return s;
}

