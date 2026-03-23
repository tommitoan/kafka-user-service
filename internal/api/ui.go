package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func RegisterUI(r *gin.Engine) {
	r.GET("/", func(c *gin.Context) {
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.String(http.StatusOK, uiHTML)
	})
}

const uiHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>User Manager</title>
<style>
  :root {
    --bg: #0f1117; --surface: #1a1d27; --border: #2a2d3e;
    --accent: #6c63ff; --accent2: #ff6584; --text: #e2e8f0;
    --muted: #64748b; --success: #10b981; --danger: #ef4444;
    --warn: #f59e0b;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif; min-height: 100vh; }
  header { background: var(--surface); border-bottom: 1px solid var(--border); padding: 16px 32px; display: flex; align-items: center; gap: 12px; }
  header h1 { font-size: 1.25rem; font-weight: 700; letter-spacing: -.5px; }
  .badge { background: var(--accent); color: #fff; font-size: .65rem; padding: 2px 8px; border-radius: 99px; font-weight: 600; text-transform: uppercase; }
  .layout { display: grid; grid-template-columns: 340px 1fr; gap: 0; height: calc(100vh - 57px); }
  .panel { padding: 24px; overflow-y: auto; }
  .panel-left { border-right: 1px solid var(--border); background: var(--surface); }
  h2 { font-size: .8rem; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; color: var(--muted); margin-bottom: 16px; }
  .form-group { margin-bottom: 14px; }
  label { display: block; font-size: .78rem; color: var(--muted); margin-bottom: 5px; font-weight: 500; }
  input { width: 100%; background: var(--bg); border: 1px solid var(--border); border-radius: 8px; padding: 9px 12px; color: var(--text); font-size: .88rem; outline: none; transition: border-color .15s; }
  input:focus { border-color: var(--accent); }
  .btn { width: 100%; padding: 10px; border: none; border-radius: 8px; font-size: .88rem; font-weight: 600; cursor: pointer; transition: opacity .15s, transform .1s; }
  .btn:active { transform: scale(.98); }
  .btn:hover { opacity: .88; }
  .btn-primary { background: var(--accent); color: #fff; }
  .btn-danger  { background: var(--danger);  color: #fff; }
  .btn-warn    { background: var(--warn);    color: #000; }
  .btn-sm { width: auto; padding: 5px 12px; font-size: .78rem; border-radius: 6px; }
  .divider { border: none; border-top: 1px solid var(--border); margin: 20px 0; }
  .search-row { display: flex; gap: 8px; margin-bottom: 20px; }
  .search-row input { flex: 1; }
  table { width: 100%; border-collapse: collapse; font-size: .85rem; }
  th { text-align: left; padding: 10px 12px; font-size: .72rem; text-transform: uppercase; letter-spacing: .8px; color: var(--muted); border-bottom: 1px solid var(--border); }
  td { padding: 12px; border-bottom: 1px solid var(--border); vertical-align: middle; }
  tr:hover td { background: rgba(108,99,255,.05); }
  .uuid { font-family: monospace; font-size: .72rem; color: var(--muted); max-width: 120px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .actions { display: flex; gap: 6px; }
  .toast { position: fixed; bottom: 24px; right: 24px; padding: 12px 20px; border-radius: 10px; font-size: .85rem; font-weight: 500; color: #fff; z-index: 999; opacity: 0; transform: translateY(10px); transition: all .25s; pointer-events: none; }
  .toast.show { opacity: 1; transform: translateY(0); }
  .toast.success { background: var(--success); }
  .toast.error   { background: var(--danger); }
  .empty { text-align: center; padding: 48px; color: var(--muted); font-size: .88rem; }
  .total { font-size: .78rem; color: var(--muted); margin-bottom: 12px; }
  .pagination { display: flex; gap: 8px; margin-top: 16px; align-items: center; }
  .pagination button { background: var(--surface); border: 1px solid var(--border); color: var(--text); padding: 6px 14px; border-radius: 6px; cursor: pointer; font-size: .82rem; }
  .pagination button:disabled { opacity: .35; cursor: default; }
  .pagination .info { font-size: .78rem; color: var(--muted); margin: 0 8px; }
  .modal-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.6); z-index: 100; align-items: center; justify-content: center; }
  .modal-overlay.open { display: flex; }
  .modal { background: var(--surface); border: 1px solid var(--border); border-radius: 14px; padding: 28px; width: 400px; max-width: 95vw; }
  .modal h3 { margin-bottom: 20px; font-size: 1rem; }
  .modal-actions { display: flex; gap: 10px; margin-top: 20px; }
  .modal-actions .btn { flex: 1; }
  .link { color: var(--accent); font-size: .82rem; text-decoration: none; display: inline-flex; align-items: center; gap: 4px; }
  .link:hover { text-decoration: underline; }
</style>
</head>
<body>

<header>
  <h1>User Manager</h1>
  <span class="badge">Kafka + Postgres</span>
  <span style="flex:1"></span>
  <a class="link" href="/swagger/index.html" target="_blank">&#x2197; Swagger UI</a>
</header>

<div class="layout">
  <!-- Left panel: Create form -->
  <div class="panel panel-left">
    <h2>Create User</h2>
    <div class="form-group">
      <label>Name *</label>
      <input id="c-name" placeholder="Alice"/>
    </div>
    <div class="form-group">
      <label>Email *</label>
      <input id="c-email" type="email" placeholder="alice@example.com"/>
    </div>
    <div class="form-group">
      <label>Age</label>
      <input id="c-age" type="number" placeholder="30" min="0" max="150"/>
    </div>
    <button class="btn btn-primary" onclick="createUser()">Create User</button>

    <hr class="divider"/>

    <h2>Get by ID</h2>
    <div class="form-group">
      <input id="g-id" placeholder="UUID"/>
    </div>
    <button class="btn btn-primary" style="background:var(--accent2)" onclick="getUser()">Fetch User</button>

    <div id="get-result" style="margin-top:14px;font-size:.82rem;display:none">
      <pre id="get-json" style="background:var(--bg);padding:12px;border-radius:8px;overflow:auto;color:var(--text)"></pre>
    </div>
  </div>

  <!-- Right panel: User table -->
  <div class="panel">
    <div class="search-row">
      <input id="search" placeholder="Search by name or email..." oninput="filterTable()"/>
      <button class="btn btn-primary" style="width:auto;padding:10px 18px" onclick="loadUsers()">Refresh</button>
    </div>

    <div class="total" id="total-label"></div>

    <table id="user-table">
      <thead>
        <tr>
          <th>ID</th>
          <th>Name</th>
          <th>Email</th>
          <th>Age</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody id="user-tbody"></tbody>
    </table>

    <div id="empty" class="empty" style="display:none">No users found.</div>

    <div class="pagination">
      <button id="btn-prev" onclick="prevPage()" disabled>&#8592; Prev</button>
      <span class="info" id="page-info"></span>
      <button id="btn-next" onclick="nextPage()">Next &#8594;</button>
    </div>
  </div>
</div>

<!-- Edit Modal -->
<div class="modal-overlay" id="edit-modal">
  <div class="modal">
    <h3>Edit User</h3>
    <input type="hidden" id="e-id"/>
    <div class="form-group">
      <label>Name</label>
      <input id="e-name" placeholder="Name"/>
    </div>
    <div class="form-group">
      <label>Email</label>
      <input id="e-email" type="email" placeholder="Email"/>
    </div>
    <div class="form-group">
      <label>Age</label>
      <input id="e-age" type="number" min="0" max="150" placeholder="Age"/>
    </div>
    <div class="modal-actions">
      <button class="btn" style="background:var(--border)" onclick="closeModal()">Cancel</button>
      <button class="btn btn-warn" onclick="updateUser()">Save Changes</button>
    </div>
  </div>
</div>

<!-- Toast -->
<div class="toast" id="toast"></div>

<script>
const API = '/api/v1';
let allUsers = [];
let offset = 0;
const limit = 10;
let total = 0;

async function loadUsers() {
  try {
    const r = await fetch(API + '/users?offset=' + offset + '&limit=' + limit);
    const data = await r.json();
    allUsers = data.users || [];
    total = data.total || 0;
    renderTable(allUsers);
    document.getElementById('total-label').textContent = total + ' user(s) total';
    document.getElementById('page-info').textContent =
      'Page ' + (Math.floor(offset/limit)+1) + ' of ' + Math.max(1, Math.ceil(total/limit));
    document.getElementById('btn-prev').disabled = offset === 0;
    document.getElementById('btn-next').disabled = offset + limit >= total;
  } catch(e) { toast('Failed to load users', 'error'); }
}

function renderTable(users) {
  const tbody = document.getElementById('user-tbody');
  const empty = document.getElementById('empty');
  if (!users.length) {
    tbody.innerHTML = '';
    empty.style.display = '';
    return;
  }
  empty.style.display = 'none';
  tbody.innerHTML = users.map(u =>
    '<tr>' +
    '<td><span class="uuid" title="' + u.id + '">' + u.id + '</span></td>' +
    '<td>' + esc(u.name) + '</td>' +
    '<td>' + esc(u.email) + '</td>' +
    '<td>' + u.age + '</td>' +
    '<td><div class="actions">' +
    '<button class="btn btn-warn btn-sm" onclick="openEdit(\'' + u.id + '\',\'' + esc(u.name) + '\',\'' + esc(u.email) + '\',' + u.age + ')">Edit</button>' +
    '<button class="btn btn-danger btn-sm" onclick="deleteUser(\'' + u.id + '\')">Delete</button>' +
    '</div></td>' +
    '</tr>'
  ).join('');
}

function filterTable() {
  const q = document.getElementById('search').value.toLowerCase();
  renderTable(allUsers.filter(u =>
    u.name.toLowerCase().includes(q) || u.email.toLowerCase().includes(q)
  ));
}

function prevPage() { offset = Math.max(0, offset - limit); loadUsers(); }
function nextPage() { offset += limit; loadUsers(); }

async function createUser() {
  const name  = document.getElementById('c-name').value.trim();
  const email = document.getElementById('c-email').value.trim();
  const age   = parseInt(document.getElementById('c-age').value) || 0;
  if (!name || !email) { toast('Name and email are required', 'error'); return; }
  try {
    const r = await fetch(API + '/users', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({name, email, age})
    });
    if (!r.ok) { const e = await r.json(); toast(e.error || 'Error', 'error'); return; }
    toast('User created!', 'success');
    document.getElementById('c-name').value = '';
    document.getElementById('c-email').value = '';
    document.getElementById('c-age').value = '';
    loadUsers();
  } catch(e) { toast('Request failed', 'error'); }
}

async function getUser() {
  const id = document.getElementById('g-id').value.trim();
  if (!id) { toast('Enter a UUID', 'error'); return; }
  try {
    const r = await fetch(API + '/users/' + id);
    const data = await r.json();
    const el = document.getElementById('get-result');
    el.style.display = '';
    document.getElementById('get-json').textContent = JSON.stringify(data, null, 2);
    if (!r.ok) toast(data.error || 'Not found', 'error');
  } catch(e) { toast('Request failed', 'error'); }
}

function openEdit(id, name, email, age) {
  document.getElementById('e-id').value = id;
  document.getElementById('e-name').value = name;
  document.getElementById('e-email').value = email;
  document.getElementById('e-age').value = age;
  document.getElementById('edit-modal').classList.add('open');
}

function closeModal() {
  document.getElementById('edit-modal').classList.remove('open');
}

async function updateUser() {
  const id    = document.getElementById('e-id').value;
  const name  = document.getElementById('e-name').value.trim();
  const email = document.getElementById('e-email').value.trim();
  const age   = parseInt(document.getElementById('e-age').value) || 0;
  try {
    const r = await fetch(API + '/users/' + id, {
      method: 'PUT',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({name, email, age})
    });
    if (!r.ok) { const e = await r.json(); toast(e.error || 'Error', 'error'); return; }
    toast('User updated!', 'success');
    closeModal();
    loadUsers();
  } catch(e) { toast('Request failed', 'error'); }
}

async function deleteUser(id) {
  if (!confirm('Delete this user?')) return;
  try {
    const r = await fetch(API + '/users/' + id, { method: 'DELETE' });
    if (r.status === 204) { toast('User deleted', 'success'); loadUsers(); }
    else { const e = await r.json(); toast(e.error || 'Error', 'error'); }
  } catch(e) { toast('Request failed', 'error'); }
}

function toast(msg, type) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = 'toast ' + type + ' show';
  setTimeout(() => el.className = 'toast', 3000);
}

function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// Close modal on overlay click
document.getElementById('edit-modal').addEventListener('click', function(e) {
  if (e.target === this) closeModal();
});

loadUsers();
</script>
</body>
</html>`
