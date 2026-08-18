const APP_VERSION = '1.1.7';
const PAGE = document.body.dataset.page;
const $ = id => document.getElementById(id);
const esc = value => String(value || '').replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
const api = async (path, options = {}) => {
  const response = await fetch('/api' + path, Object.assign({}, options, { credentials: 'same-origin', headers: Object.assign({}, options.headers || {}) }));
  const data = await response.json().catch(() => ({}));
  if (response.status === 401 && path.indexOf('/auth/') !== 0) {
    localStorage.removeItem('dualroute-authenticated');
    showLogin();
  }
  if (!response.ok) throw new Error(data.detail || data.error || ('HTTP ' + response.status));
  return data;
};
const PAGE_META = {
  instances: ['GATEWAY FLEET / OPERATIONS', '集中查看双上游实例、出口和流量池状态。'],
  mihomo: ['PROXY CONVERSION / MIHOMO', '管理订阅节点、健康状态和可分配的 SOCKS5 出口。'],
  keys: ['ACCESS CONTROL / API KEYS', '一组访问密钥可访问所有已部署上游模型。'],
  logs: ['OBSERVABILITY / ACTIVITY', '查看控制面、Mihomo 和实例运行事件。'],
  tokens: ['USAGE ANALYTICS / TOKENS', '按接口、模型和网关实例统计 API 用量。']
};
let state = {};
let mihomoPage = 0;
let selectedLogSource = 'control';

function timeStamp() { $('updated').textContent = '更新于 ' + new Date().toLocaleTimeString('zh-CN'); }
function activeSlots(instance) {
  const slots = instance.slots || [];
  const active = slots.filter(slot => slot.active && slot.enabled !== false && slot.healthy !== false);
  if (active.length) return active;
  const healthy = slots.filter(slot => slot.enabled !== false && slot.healthy !== false);
  return healthy.length ? healthy.slice(0, 1) : slots.slice(0, 1);
}
function statusBadge(value, good) { return '<span class="badge ' + (good ? 'ok' : 'wait') + '">' + esc(value) + '</span>'; }
function compactKey(value) {
  value = String(value || '');
  return value.length > 16 ? value.slice(0, 6) + '...' + value.slice(-4) : value;
}
function instanceActions(instance) {
  const name = esc(instance.instance);
  const run = instance.status === 'running';
  const slots = instance.slots || [];
  const canRotate = slots.length > 1 && instance.online;
  return '<div class="row-actions"><button class="icon-button instance-settings" data-name="' + name + '" title="设置">⚙</button>' +
    (canRotate ? '<button class="icon-button instance-action" data-name="' + name + '" data-action="rotate" title="更换出口 IP">⇄</button>' : '') +
    (run ? '<button class="icon-button instance-action" data-name="' + name + '" data-action="stop" title="停止">Ⅱ</button>' : '<button class="icon-button instance-action" data-name="' + name + '" data-action="start" title="启动">▶</button>') +
    '<button class="icon-button instance-action" data-name="' + name + '" data-action="restart" title="重启">↻</button><button class="icon-button danger instance-delete" data-name="' + name + '" title="删除">×</button></div>';
}
function renderInstances() {
  const instances = state.instances || [];
  $('instances').textContent = instances.length;
  $('online').textContent = instances.filter(item => item.online).length;
  $('concurrency').textContent = state.max_concurrency || 0;
  $('rate').textContent = state.stats && state.stats.upstream429 || 0;
  const egressOwners = new Map();
  instances.forEach(item => activeSlots(item).forEach(slot => { if (slot.egress) { const ownerKey = (item.provider || 'tokenrouter') + '|' + slot.egress; egressOwners.set(ownerKey, [...(egressOwners.get(ownerKey) || []), item.instance]); } }));
  $('fleet').innerHTML = instances.map(item => {
    const provider = item.provider === 'opencode' ? 'OpenCode' : 'TokenRouter';
    const exits = activeSlots(item).map(slot => {
      const value = esc(slot.egress || (slot.direct ? '直连（未探测）' : '代理未探测'));
      const node = slot.mihomo_node ? '<small class="muted" style="display:block;margin-top:4px">' + esc(slot.mihomo_node) + ' → ' + value + '</small>' : value;
      const duplicate = slot.egress && (egressOwners.get((item.provider || 'tokenrouter') + '|' + slot.egress) || []).length > 1;
      const cooldowns = (slot.model_cooldowns || []).map(item => '<span class="badge wait" title="恢复时间 ' + esc(item.ready_at || '') + '">' + esc(item.model || '模型') + ' 冷却</span>').join(' ');
      return node + (duplicate ? ' <span class="badge wait" title="重复出口，控制面会自动切换">重复出口</span>' : '') + (cooldowns ? ' ' + cooldowns : '') + (item.slots && item.slots.length > 1 ? ' <span class="badge pool" title="仅在 429 或故障时切换">' + item.slots.length + ' 个候选</span>' : '');
    }).join(', ') || '-';
    const maskedKey = item.provider === 'opencode' && item.auth_mode === 'public' ? 'public（公共 Key）' : compactKey(item.upstream_api_key_masked || '未设置');
    return '<tr><td><strong>' + esc(item.instance) + '</strong><small class="mono muted">' + esc((item.container_id || '').slice(0, 12) || '-') + '</small></td><td>' + statusBadge(item.online ? '在线' : (item.status || '停止'), item.online) + ' ' + statusBadge(item.in_traffic_pool ? '流量池' : '未接入', item.in_traffic_pool) + '</td><td>' + statusBadge(provider, item.provider !== 'opencode') + '</td><td class="mono" title="' + esc(item.upstream_api_key_masked || '') + '">' + esc(maskedKey) + '</td><td>' + (Number(item.max_concurrency) || 0) + '</td><td class="mono">' + exits + '</td><td>' + instanceActions(item) + '</td></tr>';
  }).join('') || '<tr><td colspan="7">暂无实例，请新建实例并设置上游 API Key。</td></tr>';
}
function syncAuthMode(form) {
  const openCode = form.elements.provider.value === 'opencode';
  const publicAccess = openCode && form.elements.auth_mode.value === 'public';
  form.querySelectorAll('[data-auth-mode]').forEach(node => { node.hidden = !openCode; });
  if (!openCode) form.elements.auth_mode.value = 'custom';
  form.querySelectorAll('[data-custom-key]').forEach(node => { node.hidden = publicAccess; });
  form.elements.upstream_api_key.required = form.id === 'createForm' && !publicAccess;
}
function syncMihomoCandidateText(form) {
  const list = $(form.id === 'settingsForm' ? 'settingsMihomoChoices' : 'createMihomoChoices');
  if (!list) return false;
  const inputs = [...list.querySelectorAll('input[type="checkbox"]')];
  if (!inputs.length) return false;
  form.elements.proxy_urls.value = inputs.filter(input => input.checked && !input.disabled).map(input => input.value).join('\n');
  return true;
}
function renderMihomoCandidates(form, result) {
  const list = $(form.id === 'settingsForm' ? 'settingsMihomoChoices' : 'createMihomoChoices');
  if (!list) return;
  const endpoints = result.endpoints || [];
  const fallbackURLs = result.proxy_urls || [];
  const candidates = (endpoints.length ? endpoints.map((endpoint, index) => ({
    url: endpoint.url || fallbackURLs[index] || '',
    node: endpoint.active_node || endpoint.node || '节点状态未知',
    healthy: endpoint.healthy !== false
  })) : fallbackURLs.map(url => ({ url, node: 'Mihomo 出口', healthy: true }))).filter(item => item.url);
  if (!candidates.length) {
    list.innerHTML = '<span class="muted">请先在 Mihomo 转换页面保存订阅。</span>';
    return;
  }
  const existing = new Set((form.elements.proxy_urls.value || '').split(/\r?\n|,/).map(value => value.trim()).filter(Boolean));
  const provider = form.elements.provider.value || 'tokenrouter';
  const editingInstance = form.elements.name?.value || '';
  const owners = new Map();
  (state.instances || []).forEach(instance => {
    if ((instance.provider || 'tokenrouter') !== provider || instance.instance === editingInstance) return;
    (instance.proxy_urls || []).forEach(url => {
      const normalized = String(url || '').trim();
      if (normalized && !owners.has(normalized)) owners.set(normalized, instance.instance);
    });
  });
  list.innerHTML = candidates.map(item => {
    const owner = owners.get(item.url);
    const blocked = !item.healthy || Boolean(owner);
    const stateLabel = owner ? '已被 ' + owner + ' 使用' : (item.healthy ? '可用' : '不可用');
    return '<label class="mihomo-choice ' + (blocked ? 'unhealthy' : 'healthy') + '"><input type="checkbox" value="' + esc(item.url) + '"' + (blocked ? ' disabled' : '') + (existing.has(item.url) ? ' checked' : '') + '><span class="health-dot" aria-hidden="true"></span><span class="choice-port">' + esc(item.url.split(':').pop() || '-') + '</span><code>' + esc(item.url) + '</code><span class="choice-state">' + esc(stateLabel) + '</span></label>';
  }).join('');
  list.querySelectorAll('input[type="checkbox"]').forEach(input => { input.onchange = () => syncMihomoCandidateText(form); });
  if (existing.size > 0) syncMihomoCandidateText(form);
}
async function loadMihomoCandidates(form) {
  const list = $(form.id === 'settingsForm' ? 'settingsMihomoChoices' : 'createMihomoChoices');
  if (list) list.innerHTML = '<span class="muted">正在读取订阅节点...</span>';
  try { renderMihomoCandidates(form, await api('/mihomo')); }
  catch (_) { if (list) list.innerHTML = '<span class="muted">Mihomo 状态暂时不可用</span>'; }
}
function resetCreateForm() {
  const form = $('createForm');
  if (!form) return;
  form.reset();
  form.elements.provider.value = 'tokenrouter';
  form.elements.auth_mode.value = 'custom';
  form.elements.upstream_api_key.value = '';
  form.elements.proxy_urls.value = '';
  form.elements.max_concurrency.value = '4';
  form.elements.queue_size.value = '8';
  syncAuthMode(form);
  const choices = $('createMihomoChoices');
  if (choices) choices.innerHTML = '<span class="muted">正在读取订阅出口...</span>';
}
async function loadInstances() { state = await api('/overview'); renderInstances(); }
function renderMihomo() {
  const endpoints = state.endpoints || [];
  const pageSize = 10;
  const pages = Math.max(1, Math.ceil(endpoints.length / pageSize));
  mihomoPage = Math.max(0, Math.min(mihomoPage, pages - 1));
  const visible = endpoints.slice(mihomoPage * pageSize, (mihomoPage + 1) * pageSize);
  $('mihomoProxy').innerHTML = visible.map(endpoint => {
    const healthy = endpoint.healthy === true || endpoint.alive === true;
    return '<div class="proxy-endpoint"><div class="proxy-line"><code>' + esc(endpoint.url) + '</code><span class="badge ' + (healthy ? 'ok' : 'bad') + '">' + (healthy ? '健康' : '不可用') + '</span></div><span title="' + esc(endpoint.active_node || endpoint.node || '') + '">' + esc(endpoint.active_node || endpoint.node || '节点状态未知') + (endpoint.type ? ' · ' + esc(endpoint.type) : '') + '</span></div>';
  }).join('') || '<span class="muted">尚未生成代理入口</span>';
  $('mihomoPage').textContent = endpoints.length ? (mihomoPage + 1) + ' / ' + pages + ' · 共 ' + endpoints.length + ' 个' : '0 / 0';
  $('mihomoPrev').disabled = mihomoPage === 0;
  $('mihomoNext').disabled = mihomoPage >= pages - 1;
}
async function loadMihomo() {
  state = await api('/mihomo');
  const running = state.status === 'running';
  $('mihomoStatus').textContent = running ? '运行中 · ' + (state.node_count || 0) + ' 出口' : (state.status || '未知');
  $('mihomoStatus').className = 'badge ' + (running ? 'ok' : 'wait');
  renderMihomo();
}
async function loadKeys() {
  const result = await api('/keys');
  state.keys = result.keys || [];
  $('keys').innerHTML = state.keys.map(key => '<div class="key"><code>' + esc(key) + '</code><button class="icon-button danger" data-key="' + esc(key) + '" title="删除密钥">×</button></div>').join('') || '<p class="muted">暂无密钥，请手动添加。</p>';
}
function renderLogs() {
  const sources = state.log_sources || [];
  if (!sources.some(source => source.id === selectedLogSource)) selectedLogSource = sources[0] ? sources[0].id : 'control';
  $('logTabs').innerHTML = sources.map(source => '<button class="log-source ' + (source.id === selectedLogSource ? 'active' : '') + '" data-source="' + esc(source.id) + '">' + esc(source.label) + '<span>' + (source.entries || []).length + '</span></button>').join('');
  const source = sources.find(item => item.id === selectedLogSource);
  const entries = (source && source.entries || []).slice(-150).reverse();
  $('logs').innerHTML = entries.map(entry => {
    if (entry.kind === 'audit') {
      const attempts = Array.isArray(entry.attempt_history) ? entry.attempt_history : [];
      const attemptCount = Math.max(Number(entry.attempts) || 1, attempts.length || 1);
      const successful = Number(entry.status) >= 200 && Number(entry.status) < 400;
      const recovered = Boolean(entry.recovered);
      const outcome = recovered ? '第' + attemptCount + '次成功 · 已切换出口' : successful ? '第' + attemptCount + '次成功' : '第' + attemptCount + '次失败';
      const trace = attempts.length > 1 ? '<details class="attempt-trace"><summary>' + attempts.length + ' 次尝试明细' + (entry.request_id ? ' · ' + esc(String(entry.request_id).slice(0, 8)) : '') + '</summary><div class="attempt-list">' + attempts.map(attempt => '<div class="attempt-row"><b class="status-' + (Number(attempt.status) >= 200 && Number(attempt.status) < 400 ? 'ok' : 'bad') + '">' + (Number(attempt.status) || 0) + '</b><span>第' + (Number(attempt.attempt) || 1) + '次 · ' + esc(attempt.egress || '出口未知') + ' · ' + esc(attempt.source || 'upstream') + '</span><time>' + (Number(attempt.latency_ms) || 0) + 'ms</time></div>').join('') + '</div></details>' : '';
      return '<div class="log audit ' + (recovered ? 'recovered' : '') + '"><b class="status-' + (successful ? 'ok' : 'bad') + '">' + (Number(entry.status) || 0) + '</b><span class="mono log-message">' + esc(entry.method) + ' ' + esc(entry.path) + (entry.model ? ' · ' + esc(entry.model) : '') + '</span><span class="log-outcome">' + esc(entry.instance || '-') + ' · ' + esc(entry.egress || '出口未知') + ' · ' + outcome + '</span><time>' + (Number(entry.latency_ms) || 0) + 'ms</time>' + trace + '</div>';
    }
    return '<div class="log"><b class="level-' + esc(entry.level || 'info') + '">' + esc(entry.level || 'info') + '</b><span class="mono log-message" title="' + esc(entry.message || '') + '">' + esc(entry.message || '-') + '</span><time>' + (entry.at ? new Date(entry.at).toLocaleTimeString('zh-CN') : '-') + '</time></div>';
  }).join('') || '<p class="empty-state">暂无日志</p>';
}
async function loadLogs() { state = await api('/overview'); renderLogs(); }
const formatNumber = value => new Intl.NumberFormat('zh-CN').format(Number(value) || 0);
const formatUSD = value => '$' + (Number(value) || 0).toFixed(6);
const displayTime = value => value ? new Date(value).toLocaleTimeString('zh-CN') : '-';
const cacheHitRate = (cached, prompt) => Number(prompt) > 0 ? Math.min(100, Math.max(0, (Number(cached || 0) / Number(prompt)) * 100)).toFixed(1) + '%' : '-';
function outputSpeed(record) {
  if (!record.stream || !Number(record.completion_tokens) || !Number(record.first_token_ms)) return '';
  const generationMS = Number(record.latency_ms) - Number(record.first_token_ms);
  return generationMS > 0 ? (Number(record.completion_tokens) / (generationMS / 1000)).toFixed(1) + ' t/s' : '';
}
function costDetails(record) {
  const total = formatUSD(record.total_cost_usd);
  return '<details class="cost-details"><summary>' + total + '</summary><div class="cost-popover"><strong>费用明细</strong><div><span>输入费用</span><b>' + formatUSD(record.input_cost_usd) + '</b></div><div><span>输出费用</span><b>' + formatUSD(record.output_cost_usd) + '</b></div><div><span>缓存读取费用</span><b>' + formatUSD(record.cache_cost_usd) + '</b></div><hr><div><span>总费用</span><b>' + total + '</b></div><small>USD · 按模型费率估算</small></div></details>';
}
function tokenQuery() {
  const query = new URLSearchParams();
  [['instance', 'tokenInstance'], ['path', 'tokenPath'], ['model', 'tokenModel'], ['key', 'tokenKey'], ['status', 'tokenStatus']].forEach(pair => { if ($(pair[1]).value) query.set(pair[0], $(pair[1]).value); });
  return query.toString() ? '/tokens?' + query.toString() : '/tokens';
}
function syncTokenFilters(records) {
  [['tokenInstance', 'instance'], ['tokenPath', 'path'], ['tokenModel', 'model'], ['tokenKey', 'client_key']].forEach(pair => {
    const select = $(pair[0]);
    const selected = select.value;
    const options = [...new Set(records.map(record => record[pair[1]]).filter(Boolean))].sort();
    select.innerHTML = '<option value="">全部</option>' + options.map(value => '<option value="' + esc(value) + '">' + esc(value) + '</option>').join('');
    select.value = options.includes(selected) ? selected : '';
  });
}
function renderTokens(data) {
	const summary = data.summary || {};
	const records = data.records || [];
  syncTokenFilters(records);
  const instanceView = Boolean($('tokenInstance').value);
  $('tokenSummary').classList.toggle('instance-view', instanceView);
  document.querySelectorAll('[data-token-detail]').forEach(card => { card.hidden = instanceView; });
  $('tokenRequests').textContent = formatNumber(summary.requests);
  $('tokenSuccess').textContent = '成功 ' + formatNumber(summary.success) + ' · 异常 ' + formatNumber(summary.errors);
  $('tokenTotal').textContent = formatNumber(summary.total_tokens);
  $('tokenPrompt').textContent = formatNumber(summary.prompt_tokens);
  $('tokenCompletion').textContent = formatNumber(summary.completion_tokens);
  $('tokenCached').textContent = formatNumber(summary.cached_tokens);
  $('tokenCacheRate').textContent = cacheHitRate(summary.cached_tokens, summary.prompt_tokens);
  $('tokenCost').textContent = formatUSD(summary.total_cost_usd);
  $('tokenRows').innerHTML = records.map(record => {
    const success = Number(record.status) >= 200 && Number(record.status) < 400;
    const hasUsage = Number(record.total_tokens) || Number(record.prompt_tokens) || Number(record.completion_tokens) || Number(record.cached_tokens);
    const speed = outputSpeed(record);
    const firstToken = Number(record.first_token_ms) ? (Number(record.first_token_ms) / 1000).toFixed(1) + 's' : '-';
    return '<tr><td><time>' + esc(record.at ? new Date(record.at).toLocaleTimeString('zh-CN') : '-') + '</time><small>' + esc(record.at ? new Date(record.at).toLocaleDateString('zh-CN') : '') + '</small></td><td><strong>' + esc(record.instance || '-') + '</strong><small class="mono">' + esc(record.egress || '出口未知') + '</small></td><td><code class="path-chip">' + esc(record.path || '-') + '</code></td><td><code class="key-chip">' + esc(record.client_key || '旧记录') + '</code></td><td><span class="model-chip" title="' + esc(record.model || '') + '">' + esc(record.model || '-') + '</span></td><td><span class="stream-state ' + (record.stream ? 'streaming' : '') + '">' + (record.stream ? '流' : '非流') + '</span><small>' + (speed || '-') + '</small></td><td class="usage-cell">' + (hasUsage ? '<strong>输入 ' + formatNumber(record.prompt_tokens) + ' / 输出 ' + formatNumber(record.completion_tokens) + '</strong><small>' + (Number(record.cached_tokens) ? '缓存 ' + formatNumber(record.cached_tokens) : '未命中缓存') + '</small>' : '<span class="muted">未返回用量</span>') + '</td><td>' + costDetails(record) + '</td><td><span class="status-chip ' + (success ? 'ok' : 'bad') + '">' + (Number(record.status) || '-') + '</span><small>' + esc(record.source || 'upstream') + (record.recovered ? ' · 已恢复' : '') + '</small></td><td class="latency-cell"><i class="' + (success ? 'ok' : 'bad') + '"></i><strong>首字 ' + firstToken + '</strong><small>耗时 ' + (Number(record.latency_ms) / 1000).toFixed(1) + 's · 第' + (Number(record.attempts) || 1) + '次</small></td></tr>';
  }).join('') || '<tr><td colspan="10"><p class="empty-state">当前筛选条件下暂无接口统计记录</p></td></tr>';
}
async function loadTokens() { renderTokens(await api(tokenQuery())); }
async function loadPage() {
  try {
    if (PAGE === 'instances') await loadInstances();
    if (PAGE === 'mihomo') await loadMihomo();
    if (PAGE === 'keys') await loadKeys();
    if (PAGE === 'logs') await loadLogs();
    if (PAGE === 'tokens') await loadTokens();
    timeStamp();
  } catch (error) { if (error.message !== 'unauthorized') alert('刷新失败：' + error.message); }
}
function bindPage() {
  if (PAGE === 'instances') {
    ['createForm', 'settingsForm'].forEach(id => { const form = $(id); syncAuthMode(form); form.elements.provider.onchange = () => { syncAuthMode(form); loadMihomoCandidates(form); }; form.elements.auth_mode.onchange = () => syncAuthMode(form); form.elements.proxy_urls.addEventListener('input', () => { const selected = new Set(form.elements.proxy_urls.value.split(/\r?\n|,/).map(value => value.trim()).filter(Boolean)); const list = $(form.id === 'settingsForm' ? 'settingsMihomoChoices' : 'createMihomoChoices'); if (list) list.querySelectorAll('input[type="checkbox"]').forEach(input => { input.checked = selected.has(input.value); }); }); });
    $('openCreate').onclick = () => { resetCreateForm(); $('createDialog').showModal(); loadMihomoCandidates($('createForm')); };
    $('instanceProbe').onclick = loadPage;
    $('createForm').onsubmit = instanceSubmit('/instances', 'POST', 'createDialog');
    $('settingsForm').onsubmit = instanceSubmit('', 'PUT', 'settingsDialog');
  }
  if (PAGE === 'mihomo') {
    $('mihomoForm').onsubmit = async event => { event.preventDefault(); try { await api('/mihomo', { method: 'PUT', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ subscription_url: new FormData(event.currentTarget).get('subscription_url').trim() }) }); loadPage(); } catch (error) { alert(error.message); } };
    $('mihomoProbe').onclick = async () => { await api('/mihomo/probe', { method: 'POST' }); loadPage(); };
    $('mihomoCopy').onclick = () => navigator.clipboard && navigator.clipboard.writeText((state.proxy_urls || []).join('\n'));
    $('mihomoClear').onclick = async () => { if (confirm('清除订阅并停止使用全部 Mihomo 节点？')) { await api('/mihomo', { method: 'PUT', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ clear: true }) }); loadPage(); } };
    $('mihomoPrev').onclick = () => { mihomoPage--; renderMihomo(); };
    $('mihomoNext').onclick = () => { mihomoPage++; renderMihomo(); };
  }
  if (PAGE === 'keys') {
    $('newKey').onclick = () => $('keyDialog').showModal();
    $('keyForm').onsubmit = async event => { if (event.submitter && event.submitter.value === 'cancel') return; event.preventDefault(); try { const result = await api('/keys', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ key: new FormData(event.currentTarget).get('key').trim() }) }); $('keyDialog').close(); if (navigator.clipboard) await navigator.clipboard.writeText(result.key); alert('已添加并同步：\n' + result.key); loadPage(); } catch (error) { alert('添加失败：' + error.message); } };
  }
  if (PAGE === 'tokens') $('tokenFilterApply').onclick = loadPage;
}
function instanceSubmit(path, method, dialog) {
  return async event => {
    if (event.submitter && event.submitter.value === 'cancel') return;
    event.preventDefault();
    const form = event.currentTarget;
    syncMihomoCandidateText(form);
    const data = new FormData(form);
    const target = path || '/instances/' + encodeURIComponent(data.get('name'));
    const payload = { name: data.get('name').trim(), provider: data.get('provider'), auth_mode: data.get('auth_mode'), upstream_api_key: data.get('upstream_api_key').trim(), max_concurrency: Number(data.get('max_concurrency')), queue_size: Number(data.get('queue_size')), proxy_urls: data.get('proxy_urls').split(/\r?\n|,/).map(value => value.trim()).filter(Boolean) };
    try { await api(target, { method: method, headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload) }); $(dialog).close(); loadPage(); } catch (error) { alert('保存失败：' + error.message); }
  };
}
document.addEventListener('click', async event => {
  const source = event.target.closest('.log-source');
  if (source) { selectedLogSource = source.dataset.source; renderLogs(); return; }
  const useMihomo = event.target.closest('.use-mihomo');
  if (useMihomo) {
    const form = $(useMihomo.dataset.form);
    const list = $(form.id === 'settingsForm' ? 'settingsMihomoChoices' : 'createMihomoChoices');
    const values = [...(list ? list.querySelectorAll('input[type="checkbox"]:not(:disabled)') : [])].map(input => input.value);
    if (!values.length) { alert('请先在 Mihomo 页面保存有效订阅。'); return; }
    list.querySelectorAll('input[type="checkbox"]:not(:disabled)').forEach(input => { input.checked = true; });
    form.elements.proxy_urls.value = values.join('\n');
    return;
  }
  const key = event.target.dataset.key;
  if (key && confirm('从全部实例删除该密钥？')) { await api('/keys/' + encodeURIComponent(key), { method: 'DELETE' }); loadPage(); return; }
  const settings = event.target.closest('.instance-settings');
  if (settings) {
    const item = (state.instances || []).find(instance => instance.instance === settings.dataset.name);
    if (!item) return;
    const form = $('settingsForm');
    form.elements.name.value = item.instance; form.elements.provider.value = item.provider || 'tokenrouter'; form.elements.auth_mode.value = item.auth_mode || 'custom'; form.elements.upstream_api_key.value = ''; form.elements.proxy_urls.value = (item.proxy_urls || []).join('\n'); form.elements.max_concurrency.value = item.max_concurrency || 4; form.elements.queue_size.value = item.queue_size ?? 8;
    $('settingsHint').textContent = item.instance + ' 当前上游 API Key：' + (item.upstream_api_key_masked || '未设置') + '；留空保持不变。';
    syncAuthMode(form); $('settingsDialog').showModal(); loadMihomoCandidates(form); return;
  }
  const action = event.target.closest('.instance-action');
  if (action) { try { await api('/instances/' + action.dataset.name + '/' + action.dataset.action, { method: 'POST' }); loadPage(); } catch (error) { alert('操作失败：' + error.message); } return; }
  const remove = event.target.closest('.instance-delete');
  if (remove && confirm('永久删除 ' + remove.dataset.name + '？此操作不可恢复。')) { await api('/instances/' + remove.dataset.name, { method: 'DELETE' }); loadPage(); }
});
function showApp() {
  $('loadingScreen').hidden = true; $('authScreen').hidden = true; $('passwordScreen').hidden = true; document.querySelector('.app-shell').hidden = false;
  const meta = PAGE_META[PAGE] || PAGE_META.instances;
  $('pageEyebrow').textContent = meta[0]; $('pageSubtitle').textContent = meta[1]; $('versionStatus').textContent = 'v' + APP_VERSION;
  document.querySelectorAll('[data-page-link]').forEach(link => link.classList.toggle('active', link.dataset.pageLink === PAGE));
  $('pageRefresh').onclick = loadPage;
  bindPage(); loadPage();
}
function showLogin() {
  $('loadingScreen').hidden = true;
  $('passwordScreen').hidden = true;
  $('authScreen').hidden = false;
  document.querySelector('.app-shell').hidden = true;
}
function initTheme() {
  const saved = localStorage.getItem('dualroute-theme') || localStorage.getItem('gateway-theme');
  document.documentElement.dataset.theme = saved === 'dark' ? 'dark' : 'light';
  const button = $('themeToggle');
  const sync = () => { const dark = document.documentElement.dataset.theme === 'dark'; button.querySelector('.theme-icon').textContent = dark ? '☀' : '☾'; button.querySelector('.theme-label').textContent = dark ? '浅色模式' : '深色模式'; };
  button.onclick = () => { const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark'; document.documentElement.dataset.theme = next; localStorage.setItem('dualroute-theme', next); sync(); };
  sync();
}
$('loginForm').onsubmit = async event => { event.preventDefault(); const data = new FormData(event.currentTarget); try { const result = await api('/auth/login', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ username: data.get('username').trim(), password: data.get('password') }) }); if (result.must_change_password) { $('authScreen').hidden = true; $('passwordScreen').hidden = false; } else { localStorage.setItem('dualroute-authenticated', '1'); showApp(); } } catch (_) { $('loginError').textContent = '用户名或密码不正确。'; } };
$('passwordForm').onsubmit = async event => { event.preventDefault(); const data = new FormData(event.currentTarget); if (data.get('password') !== data.get('confirm_password')) { $('passwordError').textContent = '两次输入的密码不一致。'; return; } try { await api('/auth/password', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ password: data.get('password') }) }); localStorage.setItem('dualroute-authenticated', '1'); showApp(); } catch (error) { $('passwordError').textContent = error.message; } };
$('logout').onclick = async () => { await api('/auth/logout', { method: 'POST' }); localStorage.removeItem('dualroute-authenticated'); window.location.reload(); };
initTheme();
(async () => { if (localStorage.getItem('dualroute-authenticated') === '1') { showApp(); return; } try { const status = await api('/auth/status'); if (!status.authenticated) { showLogin(); return; } if (status.must_change_password) { $('loadingScreen').hidden = true; $('passwordScreen').hidden = false; return; } localStorage.setItem('dualroute-authenticated', '1'); showApp(); } catch (_) { showLogin(); } })();
