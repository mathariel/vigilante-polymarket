// =====================================================================
//  VIGILANTE DE ARBITRAJES EN TIEMPO REAL · versión para GitHub Actions
//  Modo simulado: solo lee datos públicos de Polymarket, nunca opera.
//
//  Cada ejecución:
//   1. Carga el historial guardado en data/episodios.json
//   2. Se conecta en tiempo real a Polymarket durante DURACION_MIN minutos
//   3. Registra cada arbitraje (episodio): cuándo aparece, cuánto dura, cuánto rinde
//   4. Al terminar guarda data/episodios.json y data/resumen.json (lo que lee el tablero)
// =====================================================================
import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'node:fs';

const DURACION_MIN = Number(process.env.DURACION_MIN || 52);  // minutos por ejecución
const GANANCIA_MIN = 0.50;                // dólares mínimos para contar un episodio
const RETARDOS = [0.5, 2, 5, 30];         // tiempos de reacción simulados (segundos)
const TROZO = 400;                        // resultados por conexión
const MAX_EPISODIOS = 20000;              // tope del historial guardado
const GUARDAR_CADA_MIN = 5;               // guardado parcial

const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);
if (!existsSync('data')) mkdirSync('data');

// ---------- 1. Historial ----------
let episodios = [];
try { episodios = JSON.parse(readFileSync('data/episodios.json', 'utf8')); } catch { episodios = []; }
let inicioHistorial = episodios.length ? Math.min(...episodios.map(e => e.inicio)) : Date.now();
const ejecucion = { inicio: Date.now(), mensajes: 0, reconexiones: 0, errores: [] };
log(`Historial cargado: ${episodios.length} episodios`);

// ---------- Estado en memoria ----------
const libros = new Map(), tokenAEvento = new Map(), eventos = new Map(), abiertos = new Map();
let conexiones = [];

const js = (s) => { try { return JSON.parse(s); } catch { return null; } };
async function pedir(url) {
  for (let i = 0; i < 3; i++) {
    try { const r = await fetch(url, { signal: AbortSignal.timeout(15000) }); if (r.ok) return await r.json(); ejecucion.ultimoHttp = r.status; } catch (e) { ejecucion.ultimoHttp = String(e.message); }
    await new Promise(r => setTimeout(r, 1500));
  }
  throw new Error('sin respuesta de ' + url.split('?')[0] + ' (' + ejecucion.ultimoHttp + ')');
}

// ---------- 2. Universo de eventos con resultados excluyentes y completos ----------
async function cargarEventos() {
  const nuevos = new Map();
  for (let p = 0; p < 20; p++) {
    const pagina = await pedir(`https://gamma-api.polymarket.com/events?active=true&closed=false&limit=100&offset=${p * 100}&order=volume24hr&ascending=false`);
    for (const ev of pagina) {
      if (!ev.negRisk || !ev.markets || ev.markets.length < 2) continue;
      const activos = []; let completo = true;
      for (const m of ev.markets) {
        if (m.closed) { const pr = js(m.outcomePrices); if (!pr || Number(pr[0]) !== 0) completo = false; }
        else if (m.active && m.enableOrderBook) activos.push(m);
        else completo = false;
      }
      if (!completo || activos.length < 2) continue;
      const patas = activos.map(m => (js(m.clobTokenIds) || [])[0]).filter(Boolean);
      if (patas.length !== activos.length) continue;
      // tarifa de "tomador" de cada resultado: comisión = acciones x tasa x (p x (1-p))^exponente
      const tasas = activos.map(m => {
        const f = m.feeSchedule || {};
        return m.feesEnabled === false ? { tasa: 0, exp: 1 } : { tasa: Number(f.rate) || 0, exp: Number(f.exponent) || 1 };
      });
      nuevos.set(String(ev.id), { titulo: ev.title, fin: ev.endDate, patas, tasas });
    }
    if (pagina.length < 100) break;
  }
  eventos.clear(); tokenAEvento.clear();
  for (const [id, ev] of nuevos) {
    eventos.set(id, ev);
    for (const t of ev.patas) { if (!tokenAEvento.has(t)) tokenAEvento.set(t, []); tokenAEvento.get(t).push(id); }
  }
  log(`Eventos: ${eventos.size} · resultados: ${tokenAEvento.size}`);
  conectar([...tokenAEvento.keys()]);
}

// ---------- 3. Conexiones en tiempo real (WebSocket) ----------
function conectar(tokens) {
  conexiones.forEach(c => { c.vivo = false; clearInterval(c.ping); try { c.ws.close(); } catch {} });
  conexiones = []; libros.clear();
  for (let i = 0; i < tokens.length; i += TROZO) abrir(tokens.slice(i, i + TROZO));
}
function abrir(lote) {
  const c = { lote, vivo: true };
  const ws = new WebSocket('wss://ws-subscriptions-clob.polymarket.com/ws/market');
  c.ws = ws;
  ws.onopen = () => ws.send(JSON.stringify({ assets_ids: lote, type: 'market' }));
  ws.onmessage = (e) => { if (e.data === 'PONG') return; const d = js(e.data); if (!d) return; (Array.isArray(d) ? d : [d]).forEach(procesar); };
  ws.onerror = () => {};
  ws.onclose = () => {
    clearInterval(c.ping);
    if (c.vivo) { ejecucion.reconexiones++; setTimeout(() => { if (c.vivo) { conexiones = conexiones.filter(x => x !== c); abrir(lote); } }, 2000); }
  };
  c.ping = setInterval(() => { try { ws.readyState === 1 && ws.send('PING'); } catch {} }, 10000);
  conexiones.push(c);
}

// ---------- 4. Procesar cada cambio del libro de órdenes ----------
function procesar(m) {
  ejecucion.mensajes++;
  const ts = Number(m.timestamp) || Date.now();
  const tocados = new Set();
  if (m.event_type === 'book' || (m.bids && m.asks && m.asset_id)) {
    if (!tokenAEvento.has(m.asset_id)) return;
    const b = { bids: new Map(), asks: new Map() };
    (m.bids || []).forEach(x => b.bids.set(+x.price, +x.size));
    (m.asks || []).forEach(x => b.asks.set(+x.price, +x.size));
    libros.set(m.asset_id, b);
    tokenAEvento.get(m.asset_id).forEach(id => tocados.add(id));
  } else if (m.price_changes) {
    for (const c of m.price_changes) {
      if (!tokenAEvento.has(c.asset_id)) continue;
      const b = libros.get(c.asset_id); if (!b) continue;
      const lado = c.side === 'BUY' ? b.bids : b.asks;
      if (+c.size === 0) lado.delete(+c.price); else lado.set(+c.price, +c.size);
      tokenAEvento.get(c.asset_id).forEach(id => tocados.add(id));
    }
  }
  tocados.forEach(id => evaluar(id, ts));
}

// lado 'si': comprar todos los Sí (paga $1 por juego)
// lado 'no': comprar todos los No (paga $n-1); el No a precio p equivale a vender Sí a 1-p
function niveles(token, lado) {
  const b = libros.get(token); if (!b) return null;
  if (lado === 'si') return [...b.asks].filter(([, s]) => s > 0).map(([p, s]) => ({ p, s })).sort((x, y) => x.p - y.p);
  return [...b.bids].filter(([, s]) => s > 0).map(([p, s]) => ({ p: 1 - p, s })).sort((x, y) => x.p - y.p);
}
// Recorre el libro comprando juegos completos mientras convenga.
// Devuelve la ganancia BRUTA (sin comisiones) y la NETA (pagando la comisión de tomador,
// y dejando de comprar en cuanto un juego adicional ya no cubre su comisión).
function calcular(ev, lado) {
  const asks = ev.patas.map(t => niveles(t, lado));
  if (asks.some(a => !a || !a.length)) return null;
  const pago = lado === 'si' ? 1 : ev.patas.length - 1;
  if (asks.reduce((s, a) => s + a[0].p, 0) >= pago - 1e-9) return { ganancia: 0, neta: 0 };
  const comisionPorJuego = (precios) => precios.reduce((s, p, i) => s + ev.tasas[i].tasa * Math.pow(p * (1 - p), ev.tasas[i].exp), 0);
  const idx = asks.map(() => 0), resto = asks.map(a => a[0].s);
  let juegos = 0, costo = 0, netaJuegos = 0, netaCosto = 0, netaComision = 0, netaAbierta = true;
  for (let paso = 0; paso < 400; paso++) {
    if (asks.some((a, i) => idx[i] >= a.length)) break;
    const precios = asks.map((a, i) => a[idx[i]].p);
    const suma = precios.reduce((s, p) => s + p, 0);
    if (suma >= pago - 1e-9) break;
    const q = Math.min(...resto);
    juegos += q; costo += q * suma;
    const com = comisionPorJuego(precios);
    if (netaAbierta && suma + com < pago - 1e-9) { netaJuegos += q; netaCosto += q * suma; netaComision += q * com; }
    else netaAbierta = false;
    asks.forEach((a, i) => { resto[i] -= q; if (resto[i] <= 1e-9) { idx[i]++; resto[i] = idx[i] < a.length ? a[idx[i]].s : 0; } });
  }
  const neta = netaJuegos * pago - netaCosto - netaComision;
  return { ganancia: juegos * pago - costo, juegos, costo, neta: Math.max(0, neta), comision: netaComision };
}

function evaluar(id, ts) {
  const ev = eventos.get(id); if (!ev) return;
  for (const lado of ['si', 'no']) {
    const r = calcular(ev, lado);
    if (!r) continue;
    const clave = id + ':' + lado;
    const ep = abiertos.get(clave);
    if (r.ganancia >= GANANCIA_MIN) {
      if (!ep) {
        const nuevo = { id, lado, titulo: ev.titulo, n: ev.patas.length, fin: ev.fin, inicio: ts, ganInicio: +r.ganancia.toFixed(2), netaInicio: +r.neta.toFixed(2), tasa: Math.max(...ev.tasas.map(t => t.tasa)), costoInicio: +r.costo.toFixed(2), max: +r.ganancia.toFixed(2), serie: [[0, +r.ganancia.toFixed(2), +r.neta.toFixed(2)]], verificado: null };
        abiertos.set(clave, nuevo); verificar(nuevo, ev);
      } else {
        ep.max = Math.max(ep.max, +r.ganancia.toFixed(2));
        if (ep.serie.length < 40) ep.serie.push([ts - ep.inicio, +r.ganancia.toFixed(2), +r.neta.toFixed(2)]);
      }
    } else if (ep) {
      ep.dur = (ts - ep.inicio) / 1000;
      abiertos.delete(clave);
      episodios.push(ep);
      if (episodios.length > MAX_EPISODIOS) episodios.shift();
    }
  }
}

// ---------- 5. Verificación independiente con una consulta directa ----------
async function verificar(ep, ev) {
  try {
    const books = await Promise.all(ev.patas.map(t => pedir('https://clob.polymarket.com/book?token_id=' + t)));
    const tope = books.map(b => ep.lado === 'si'
      ? Math.min(...(b.asks || []).map(a => +a.price), 9)
      : Math.min(...(b.bids || []).map(a => 1 - +a.price), 9));
    const pago = ep.lado === 'si' ? 1 : ev.patas.length - 1;
    ep.verificado = tope.reduce((s, x) => s + x, 0) < pago - 1e-9;
  } catch { ep.verificado = 'error'; }
}

// ---------- 6. Resumen para el tablero ----------
function capturaA(ep, seg) {
  if (ep.dur <= seg) return null;
  let g = ep.ganInicio, n = ep.netaInicio || 0;
  for (const [t, v, vn] of ep.serie) { if (t <= seg * 1000) { g = v; n = vn || 0; } }
  return { bruta: g, neta: n };
}
// Solo cuentan los arbitrajes de eventos que se resuelven en 7 días o menos:
// los de largo plazo (como el Mundial 2030) inmovilizan el capital por años y distorsionan las cifras.
const DIAS_MAX = 7;
const esCorto = (e) => e.fin && (new Date(e.fin).getTime() - e.inicio) <= DIAS_MAX * 864e5;
function resumen(final) {
  const verificados = episodios.filter(e => e.verificado !== false);
  const cerrados = verificados.filter(esCorto);
  const largos = new Set(verificados.filter(e => !esCorto(e)).map(e => e.titulo));
  const durs = cerrados.map(e => e.dur).sort((a, b) => a - b);
  const cubetas = [['< 0.1 s', 0, 0.1, true], ['0.1–1 s', 0.1, 1, true], ['1–5 s', 1, 5], ['5–30 s', 5, 30], ['30 s–5 min', 30, 300], ['> 5 min', 300, 1e12]];
  const grupos = new Map();
  for (const e of cerrados) {
    const g = grupos.get(e.titulo) || { titulo: e.titulo, n: 0, gan: 0, neta: 0, durs: [] };
    g.n++; g.gan += e.ganInicio; g.neta += e.netaInicio || 0; g.durs.push(e.dur); grupos.set(e.titulo, g);
  }
  const porHora = new Map();
  for (const e of cerrados) { const h = Math.floor(e.inicio / 3600000) * 3600000; porHora.set(h, (porHora.get(h) || 0) + 1); }
  return {
    actualizado: Date.now(), inicio: inicioHistorial,
    ejecucion: { inicio: ejecucion.inicio, fin: final ? Date.now() : null, mensajes: ejecucion.mensajes, reconexiones: ejecucion.reconexiones, eventos: eventos.size, errores: ejecucion.errores.slice(-5) },
    total: cerrados.length, falsos: episodios.length - verificados.length,
    largos: { episodios: verificados.length - cerrados.length, eventos: largos.size, dias: DIAS_MAX },
    mediana: durs.length ? durs[Math.floor(durs.length / 2)] : null,
    captura: RETARDOS.map(s => {
      const cap = cerrados.map(e => capturaA(e, s)).filter(x => x !== null);
      const netas = cap.filter(x => x.neta > 0);
      return { s, n: cap.length, gan: +cap.reduce((a, b) => a + b.bruta, 0).toFixed(2), nNeta: netas.length, neta: +netas.reduce((a, b) => a + b.neta, 0).toFixed(2) };
    }),
    rentables: cerrados.filter(e => (e.netaInicio || 0) > 0).length,
    duraciones: cubetas.map(([et, lo, hi, rapido]) => ({ et, rapido: !!rapido, n: durs.filter(x => x >= lo && x < hi).length })),
    porEvento: [...grupos.values()].sort((a, b) => b.n - a.n).slice(0, 10).map(g => ({ titulo: g.titulo, n: g.n, gan: +g.gan.toFixed(2), neta: +g.neta.toFixed(2), mediana: g.durs.sort((a, b) => a - b)[Math.floor(g.durs.length / 2)] })),
    porHora: [...porHora.entries()].sort((a, b) => a[0] - b[0]).slice(-72).map(([h, n]) => ({ h, n })),
    recientes: cerrados.slice(-15).reverse().map(e => ({ inicio: e.inicio, titulo: e.titulo, dur: e.dur, gan: e.ganInicio, neta: e.netaInicio || 0, lado: e.lado }))
  };
}
function guardar(final) {
  writeFileSync('data/episodios.json', JSON.stringify(episodios));
  writeFileSync('data/resumen.json', JSON.stringify(resumen(final), null, 1));
}

// ---------- Ciclo de vida de la ejecución ----------
let recarga = null, parcial = null, vigia = null;
function terminar(codigo) {
  clearInterval(recarga); clearInterval(parcial); clearInterval(vigia);
  conexiones.forEach(c => { c.vivo = false; clearInterval(c.ping); try { c.ws.close(); } catch {} });
  // los episodios aún abiertos se cierran al final de la ejecución (duración mínima conocida)
  for (const ep of abiertos.values()) { ep.dur = (Date.now() - ep.inicio) / 1000; ep.cortado = true; episodios.push(ep); }
  abiertos.clear();
  guardar(true);
  log(`Fin. Mensajes: ${ejecucion.mensajes} · episodios totales: ${episodios.length}`);
  process.exit(codigo);
}
// el bot SIEMPRE termina y guarda a los DURACION_MIN minutos, aunque algo se cuelgue
setTimeout(() => terminar(0), DURACION_MIN * 60000);

let cargado = false;
for (let intento = 1; intento <= 5 && !cargado; intento++) {
  try { await cargarEventos(); cargado = true; }
  catch (e) {
    ejecucion.errores.push(String(e.message));
    log(`Intento ${intento}: error al cargar eventos: ${e.message}`);
    await new Promise(r => setTimeout(r, 30000));
  }
}
if (!cargado) terminar(1);

recarga = setInterval(() => cargarEventos().catch(e => ejecucion.errores.push(String(e.message))), 15 * 60000);
parcial = setInterval(() => { guardar(false); log(`Mensajes: ${ejecucion.mensajes} · episodios: ${episodios.length} · vivos: ${abiertos.size}`); }, GUARDAR_CADA_MIN * 60000);

// vigía: si pasan 2 minutos sin ningún mensaje de Polymarket, reconecta todo
let ultimoConteo = 0, minutosQuietos = 0;
vigia = setInterval(() => {
  if (ejecucion.mensajes === ultimoConteo) {
    minutosQuietos++;
    if (minutosQuietos >= 2) { log('Sin mensajes en 2 minutos: reconectando'); ejecucion.reconexiones++; minutosQuietos = 0; cargarEventos().catch(e => ejecucion.errores.push(String(e.message))); }
  } else minutosQuietos = 0;
  ultimoConteo = ejecucion.mensajes;
}, 60000);
