def hash_function(x, i):
  return (x*i) % 64

def bloom_filter_insert(boom_filter, key):
  boom_filter[hash_function(key, 1)] = 1
  boom_filter[hash_function(key, 2)] = 1

def bloom_filter_check(boom_filter, key):
  bit_1 = boom_filter[hash_function(key, 1)]
  bit_2 = boom_filter[hash_function(key, 2)]
  return bit_1 == 1 and bit_2 == 1

bloom_filter = [0 for _ in range(64)]

bloom_filter_insert(bloom_filter, 1975)
bloom_filter_insert(bloom_filter, 1985)
bloom_filter_insert(bloom_filter, 1995)
bloom_filter_insert(bloom_filter, 2005)

print(bloom_filter_check(bloom_filter, 2015))
print(bloom_filter)

bloom_filter_insert(bloom_filter, 2015)
print(bloom_filter)
