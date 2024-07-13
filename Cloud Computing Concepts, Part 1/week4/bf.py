m_bits = [0 for _ in range(32)]

def hashf(x, i, m):
  return ((x**2 + x**3)*i) % m

def bf_insert(v):
  m_bits[hashf(v,1,32)] = 1
  m_bits[hashf(v,2,32)] = 1
  m_bits[hashf(v,3,32)] = 1

def bf_check(v):
  if m_bits[hashf(v,1,32)] and m_bits[hashf(v,2,32)] and m_bits[hashf(v,3,32)]:
    return True
  return False

bf_insert(2013)
bf_insert(2010)
bf_insert(2007)
bf_insert(2004)
bf_insert(2001)
bf_insert(1998)

print(m_bits)

print(bf_check(3201))

print(m_bits)
