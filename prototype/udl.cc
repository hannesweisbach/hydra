#include <iostream>
#include <typeinfo>
#include <assert.h>

#include "hydra/types.h"

int main() {

  using namespace hydra::literals;

  auto a = 15_ID;
  auto b = 12_ID;

  int c;
  uint16_t d = 0;
  uint16_t f = 3;
  const auto& type = typeid(a);



  std::cout << type.name() << " " << typeid('c').name() << typeid('b').name()
            << " " << typeid(c).name() << " "  << typeid(d).name() << std::endl;

  std::cout << hydra::hex(b - a) << " " << typeid(b - a).name() << " "
            << typeid(d - f).name() << std::endl;

  for (size_t i = 0; i < 256; i++) {
    for (size_t j = 0; j < 256; j++) {
      uint8_t a = static_cast<uint8_t>(i);
      uint8_t b = static_cast<uint8_t>(j);

      uint8_t s1 = a - b;
      uint8_t s2 = i - j;
      assert(s1 == s2);

    }
  }
}
