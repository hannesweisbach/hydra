template <typename Key, typename Data>
class HashTable {

  public:
    virtual bool contains(const Key& key) const = 0;
    virtual Data get(const Key& key) const = 0;
    virtual Data add(const Key& key, const Data& data) = 0;
    virtual Data remove(const Key& key) = 0;
};
