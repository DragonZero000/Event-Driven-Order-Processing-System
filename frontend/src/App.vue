<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { createOrder, getOrder, getStock } from './api'

const STORAGE_KEY = 'order-ids'
const PRODUCTS = ['soup', 'bread']
const FINAL =['COMPLETED', 'CANCELLED']
const STATUS_LABEL = {
  PENDING: 'Создан',
  PROCESSING: 'Товар зарезервирован, ждём оплату',
  COMPLETED: 'Выполнен',
  CANCELLED: 'Отменён',
}

const customerId = ref('customer-1')
const items = ref([{ product_id: 'soup', quantity: 1, price: 50 }])
const submitting = ref(false)
const error = ref('')
const orders = ref([])
const stock = ref({})

const total = computed(() =>
  items.value.reduce((sum, i) => sum + (Number(i.quantity) || 0) * (Number(i.price) || 0), 0),
)

const addItem = () => items.value.push({ product_id: '', quantity: 1, price: 0 })
const removeItem = (idx) => items.value.splice(idx, 1)

function loadIds() {
  try {
    return JSON.parse(localStorage.getItem(STORAGE_KEY)) || []
  } catch {
    return []
  }
}

function saveIds() {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(orders.value.map((o) => o.id)))
  } catch {
    /* localStorage может быть недоступен — список просто не переживёт перезагрузку */
  }
}

async function submit() {
  error.value = ''
  submitting.value = true
  try {
    const { order_id } = await createOrder({
      customer_id: customerId.value,
      items: items.value.map((i) => ({
        product_id: i.product_id.trim(),
        quantity: Number(i.quantity),
        price: Number(i.price),
      })),
    })
    orders.value.unshift({ id: order_id, status: 'PENDING', total_price: total.value })
    saveIds()
    await refreshOrder(orders.value[0])
    if (FINAL.includes(orders.value[0].status)) PRODUCTS.forEach(checkStock)
  } catch (e) {
    error.value = e.message
  } finally {
    submitting.value = false
  }
}

async function refreshOrder(order) {
  try {
    Object.assign(order, await getOrder(order.id))
  } catch {
    /* сервис временно недоступен — повторим на следующем тике */
  }
}

async function poll() {
  const active = orders.value.filter((o) => !FINAL.includes(o.status))
  await Promise.all(active.map(refreshOrder))
  // Заказ завершился (в т.ч. отменился с возвратом резерва) — остатки изменились.
  if (active.some((o) => FINAL.includes(o.status))) PRODUCTS.forEach(checkStock)
}

async function checkStock(product) {
  try {
    const r = await getStock(product)
    stock.value[product] = r.stock_quantity
  } catch (e) {
    error.value = e.message
  }
}

let timer
onMounted(async () => {
  orders.value = loadIds().map((id) => ({ id, status: 'PENDING' }))
  await poll()
  timer = setInterval(poll, 2000)
  PRODUCTS.forEach(checkStock)
})
onUnmounted(() => clearInterval(timer))
</script>

<template>
  <main class="container">
    <h1>Система обработки заказов</h1>
    <p class="muted">Event-Driven Architecture · Kafka · gRPC</p>

    <section class="card">
      <h2>Новый заказ</h2>
      <form @submit.prevent="submit">
        <label>
          Клиент
          <input v-model="customerId" required />
        </label>

        <div v-for="(item, idx) in items" :key="idx" class="row">
          <input v-model="item.product_id" placeholder="Товар (soup, bread)" required />
          <input v-model.number="item.quantity" type="number" min="1" step="1" required title="Количество" />
          <input v-model.number="item.price" type="number" min="0.01" step="0.01" required title="Цена" />
          <button type="button" class="ghost" :disabled="items.length === 1" @click="removeItem(idx)">×</button>
        </div>

        <div class="actions">
          <button type="button" class="ghost" @click="addItem">+ Позиция</button>
          <strong>Итого: {{ total.toFixed(2) }}</strong>
          <button :disabled="submitting">{{ submitting ? 'Отправка…' : 'Создать заказ' }}</button>
        </div>
        <p class="hint">Оплата отклоняется, если сумма заказа больше 200.</p>
        <p v-if="error" class="error">{{ error }}</p>
      </form>
    </section>

    <section class="card">
      <h2>Остатки на складе</h2>
      <div class="stock">
        <div v-for="p in PRODUCTS" :key="p">
          <span>{{ p }}: <strong>{{ stock[p] ?? '—' }}</strong></span>
          <button class="ghost" @click="checkStock(p)">Обновить</button>
        </div>
      </div>
    </section>

    <section class="card">
      <h2>Мои заказы</h2>
      <p v-if="!orders.length" class="muted">Пока пусто.</p>
      <table v-else>
        <thead>
          <tr><th>ID</th><th>Сумма</th><th>Статус</th></tr>
        </thead>
        <tbody>
          <tr v-for="o in orders" :key="o.id">
            <td class="mono">{{ o.id.slice(0, 8) }}</td>
            <td>{{ o.total_price != null ? Number(o.total_price).toFixed(2) : '—' }}</td>
            <td><span class="badge" :class="o.status">{{ STATUS_LABEL[o.status] || o.status }}</span></td>
          </tr>
        </tbody>
      </table>
    </section>
  </main>
</template>
