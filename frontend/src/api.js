async function request(path, options) {
  const res = await fetch(path, options)
  const body = await res.json().catch(() => ({}))
  if (!res.ok) throw new Error(body.error || `Ошибка ${res.status}`)
  return body
}

export const createOrder = (payload) =>
  request('/api/orders', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })

export const getOrder = (id) => request(`/api/orders/${encodeURIComponent(id)}`)

export const getStock = (product) => request(`/api/stock/${encodeURIComponent(product)}`)
