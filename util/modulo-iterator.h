
template <typename T, typename Fun>
const T iterate_modulo(const T begin, const T end, const T mod, Fun f) {
  for(T i = begin; i != end; i = ++i % mod) {
    if(f(i))
      return i;
  }
  return end;
}

