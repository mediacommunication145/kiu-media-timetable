/**
 * KIU — Department of Media and Communication — Timetable API (Vercel serverless)
 *
 * Same API the admin-panel page expects, backed by a Google Sheet:
 *   Timetable tab = classes, Status tab = confirm/cancel decisions,
 *   Teachers tab = teacher records, Meta tab = password/recovery/programs.
 * The Sheet is edited ONLY through this API (the panel) — it is the data
 * store, never the editing interface.
 *
 * Data lives in the project's own free Upstash Redis database (connected
 * through Vercel, which injects KV_REST_API_URL / KV_REST_API_TOKEN).
 * The admin panel is the only editing interface.
 * Env: KV_REST_API_URL, KV_REST_API_TOKEN (auto-added by Vercel),
 *      SESSION_SECRET, ADMIN_PASSWORD (initial only),
 *      ADMIN_RECOVERY_KEY (initial only).
 */
const crypto = require('crypto');

const KV_URL = process.env.KV_REST_API_URL || '';
const KV_TOKEN = process.env.KV_REST_API_TOKEN || '';
const SESSION_SECRET = process.env.SESSION_SECRET || 'kiu-session-secret';
const DEFAULT_PW = process.env.ADMIN_PASSWORD || 'kiu-admin-2025';
const DEFAULT_REC = process.env.ADMIN_RECOVERY_KEY || 'kiu-recovery-2025';

const TT_HEADER = ['Program', 'Semester', 'Section', 'Day', 'Start', 'End', 'Subject', 'Teacher', 'Classroom', 'Date'];
const ST_HEADER = ['Date', 'Subject', 'Program', 'Status', 'Reason'];
const TEACH_HEADER = ['Name', 'Designation', 'Email'];
const META_HEADER = ['Key', 'Value'];
const PROGRAMS_DEFAULT = [
  { id: 'bs-mcs', name: 'BS Media and Communication Studies' },
  { id: 'bs-dmm', name: 'BS Digital Media and Marketing' }
];
const DAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

/* ---------------- value helpers (same rules as the website) ---------------- */
function capDay(d) { d = String(d || '').trim(); return d ? d.charAt(0).toUpperCase() + d.slice(1).toLowerCase() : ''; }
function progShort(name) {
  if (/Digital Media/i.test(name || '')) return 'DM';
  if (/Media and Communication Studies/i.test(name || '')) return 'MC';
  return name || '';
}
function slug(s) { return String(s || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 40) || 'x'; }
function normTime(v) {
  if (v == null) return '';
  const s = String(v).trim();
  const dm = s.match(/Date\(\d+,\d+,\d+,(\d+),(\d+)/);
  if (dm) { const h = +dm[1], mi = +dm[2]; return (h < 10 ? '0' : '') + h + ':' + (mi < 10 ? '0' : '') + mi; }
  const m = s.match(/^(\d{1,2}):(\d{2})(?::\d{2})?\s*(AM|PM)?$/i);
  if (m) { let hh = +m[1]; if (m[3]) { if (m[3].toUpperCase() === 'PM' && hh < 12) hh += 12; if (m[3].toUpperCase() === 'AM' && hh === 12) hh = 0; } return (hh < 10 ? '0' : '') + hh + ':' + m[2]; }
  return s;
}
function normDate(v) {
  if (v == null) return '';
  const s = String(v).trim();
  const z = (x) => (x < 10 ? '0' : '') + x;
  const dm = s.match(/Date\((\d+),(\d+),(\d+)/);
  if (dm) return dm[1] + '-' + z(+dm[2] + 1) + '-' + z(+dm[3]);
  const m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
  if (m) return m[1] + '-' + z(+m[2]) + '-' + z(+m[3]);
  const d = new Date(s);
  if (!isNaN(d)) return d.getFullYear() + '-' + z(d.getMonth() + 1) + '-' + z(d.getDate());
  return '';
}
function weekdayOf(ds) { const p = String(ds).split('-'); const d = new Date(Date.UTC(+p[0], +p[1] - 1, +p[2])); return ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'][d.getUTCDay()]; }
function pktNow() {
  const fmt = new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Karachi', weekday: 'long', year: 'numeric', month: '2-digit', day: '2-digit' });
  const parts = {};
  fmt.formatToParts(new Date()).forEach(x => { parts[x.type] = x.value; });
  return { day: parts.weekday, date: parts.year + '-' + parts.month + '-' + parts.day };
}

/* ---------------- data store (Upstash Redis, REST) ---------------- */
async function kvPipeline(cmds) {
  const r = await fetch(KV_URL + '/pipeline', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + KV_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmds)
  });
  const text = await r.text();
  let j;
  try { j = JSON.parse(text); } catch (e) { throw new Error('Data store returned an unreadable reply.'); }
  if (!Array.isArray(j)) throw new Error('Data store error.');
  for (const item of j) if (item && item.error) throw new Error('Data store: ' + item.error);
  return j.map(x => (x ? x.result : null));
}
async function kvSet(key, value) { await kvPipeline([['SET', key, value]]); }
function parseJsonSafe(s, fallback) {
  if (s == null) return fallback;
  try { const v = JSON.parse(s); return v == null ? fallback : v; } catch (e) { return fallback; }
}
function stripId(o) { const c = Object.assign({}, o); delete c.id; return c; }

/* ---------------- state: Sheet tabs <-> the db the page uses ---------------- */
async function loadState() {
  const [eS, sS, tS, mS] = await kvPipeline([
    ['GET', 'kiu:entries'], ['GET', 'kiu:status'], ['GET', 'kiu:teachers'], ['GET', 'kiu:meta']
  ]);
  const entries = parseJsonSafe(eS, []).map((e, i) => {
    const x = {
      id: 'r' + (i + 2), program: String(e.program || '').trim(), semester: String(e.semester || '').replace(/[^0-9]/g, ''),
      section: String(e.section || '').trim(), day: capDay(e.day), start: normTime(e.start), end: normTime(e.end),
      subject: String(e.subject || '').trim(), teacher: String(e.teacher || '').trim(), room: String(e.room || '').trim()
    };
    const d = normDate(e.date); if (d) x.date = d;
    return x;
  });
  const statusRows = parseJsonSafe(sS, []).map(s => ({
    date: normDate(s.date), subject: String(s.subject || '').trim(), program: String(s.program || '').trim(),
    status: String(s.status || '').trim(), reason: String(s.reason || '').trim()
  })).filter(s => s.date && s.subject);
  const teacherRecords = parseJsonSafe(tS, []).map((r, i) => ({
    id: 't' + (i + 2), name: String(r.name || '').trim(), designation: String(r.designation || '').trim(), email: String(r.email || '').trim()
  })).filter(r => r.name);
  const metaMap = parseJsonSafe(mS, {});
  return { entries, statusRows, teacherRecords, metaMap };
}
function parsePrograms(metaMap) {
  try { const p = JSON.parse(metaMap.programs || ''); if (Array.isArray(p) && p.length) return p; } catch (e) {}
  return JSON.parse(JSON.stringify(PROGRAMS_DEFAULT));
}
function translateStatus(state) {
  const cancellations = [], confirmations = [];
  state.statusRows.forEach((sr, idx) => {
    if (!sr.date || !sr.subject) return;
    const low = sr.status.toLowerCase();
    const isC = low.indexOf('cancel') === 0, isK = low.indexOf('confirm') === 0;
    if (!isC && !isK) return;
    state.entries.forEach(e => {
      if (e.subject.trim().toLowerCase() !== sr.subject.toLowerCase()) return;
      if (sr.program && sr.program.toLowerCase() !== e.program.toLowerCase() && progShort(sr.program) !== progShort(e.program)) return;
      const applies = e.date ? (e.date === sr.date) : (weekdayOf(sr.date) === e.day);
      if (!applies) return;
      if (isC) cancellations.push({ id: 'x' + idx + '_' + e.id, entryId: e.id, date: sr.date, by: 'panel', reason: sr.reason || '', at: '' });
      else confirmations.push({ id: 'c' + idx + '_' + e.id, key: e.id + ':' + sr.date, entryId: e.id, date: sr.date, day: e.day, subject: e.subject, classTime: e.start, room: e.room, teacherName: e.teacher || '', teacherId: '', status: 'confirmed', decidedBy: 'panel', at: '' });
    });
  });
  return { cancellations, confirmations };
}
function buildDb(state) {
  const programs = parsePrograms(state.metaMap);
  state.entries.forEach(e => { if (e.program && !programs.some(p => p.name === e.program)) programs.push({ id: slug(e.program), name: e.program }); });
  const cat = { sections: new Set(), subjects: new Set(), teachers: new Set(), rooms: new Set() };
  state.entries.forEach(e => {
    if (e.section) cat.sections.add(e.section);
    if (e.subject) cat.subjects.add(e.subject);
    if (e.teacher) cat.teachers.add(e.teacher);
    if (e.room) cat.rooms.add(e.room);
  });
  state.teacherRecords.forEach(t => { if (t.name) cat.teachers.add(t.name); });
  const { cancellations, confirmations } = translateStatus(state);
  return {
    department: 'Department of Media and Communication', university: 'Karakoram International University',
    programs, sections: [...cat.sections], subjects: [...cat.subjects], teachers: [...cat.teachers], rooms: [...cat.rooms],
    entries: state.entries, fileMeta: null, teacherRecords: state.teacherRecords,
    cancellations, confirmations, sheets: []
  };
}
async function saveTimetable(entries) {
  await kvSet('kiu:entries', JSON.stringify(entries.map(stripId)));
}
async function saveStatus(statusRows) {
  await kvSet('kiu:status', JSON.stringify(statusRows));
}
async function saveTeachers(records) {
  await kvSet('kiu:teachers', JSON.stringify(records.map(stripId)));
}
async function saveMeta(state, key, value) {
  state.metaMap[key] = value;
  await kvSet('kiu:meta', JSON.stringify(state.metaMap));
}

/* ---------------- password / session (same rules as the original server) ---------------- */
function hashSecret(value) {
  const salt = crypto.randomBytes(16).toString('hex');
  const hash = crypto.scryptSync(String(value), salt, 32).toString('hex');
  return JSON.stringify({ salt, hash });
}
function verifySecret(value, json) {
  try {
    const rec = JSON.parse(json);
    const h = crypto.scryptSync(String(value), rec.salt, 32);
    const stored = Buffer.from(rec.hash, 'hex');
    return h.length === stored.length && crypto.timingSafeEqual(h, stored);
  } catch (e) { return false; }
}
function checkPw(state, pw) { return state.metaMap.pw ? verifySecret(pw, state.metaMap.pw) : String(pw) === DEFAULT_PW; }
function checkRec(state, key) { return state.metaMap.rec ? verifySecret(key, state.metaMap.rec) : String(key) === DEFAULT_REC; }
function makeSession() {
  const exp = String(Date.now() + 8 * 60 * 60 * 1000);
  const sig = crypto.createHmac('sha256', SESSION_SECRET).update(exp).digest('hex');
  return exp + '.' + sig;
}
function isAdmin(req) {
  const m = (req.headers.cookie || '').match(/kiu_admin=([0-9]+)\.([a-f0-9]+)/);
  if (!m) return false;
  if (+m[1] < Date.now()) return false;
  const sig = crypto.createHmac('sha256', SESSION_SECRET).update(m[1]).digest('hex');
  const a = Buffer.from(sig), b = Buffer.from(m[2]);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

/* ---------------- CSV import (same rules as the original server) ---------------- */
function parseCsv(text) {
  const rows = [];
  let row = [], cur = '', inQ = false;
  const s = String(text || '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (inQ) {
      if (ch === '"') { if (s[i + 1] === '"') { cur += '"'; i++; } else inQ = false; }
      else cur += ch;
    } else if (ch === '"') inQ = true;
    else if (ch === ',') { row.push(cur); cur = ''; }
    else if (ch === '\n') { row.push(cur); rows.push(row); row = []; cur = ''; }
    else cur += ch;
  }
  if (cur !== '' || row.length) { row.push(cur); rows.push(row); }
  return rows.filter(r => r.some(c => String(c).trim() !== ''));
}
function importCsvInto(state, text) {
  const rows = parseCsv(text);
  if (!rows.length) return { added: 0, skipped: 0, errors: ['File is empty.'] };
  const head = rows[0].map(h => String(h).trim().toLowerCase());
  const idx = (names) => head.findIndex(h => names.includes(h));
  const ci = {
    program: idx(['program', 'degree program', 'degree', 'programme']), semester: idx(['semester', 'sem']),
    section: idx(['section']), day: idx(['day']), start: idx(['start', 'start time', 'from', 'time from']),
    end: idx(['end', 'end time', 'to', 'time to']), time: idx(['time', 'timing', 'class time']),
    subject: idx(['subject', 'course', 'course name']), teacher: idx(['teacher', 'instructor', 'faculty']),
    room: idx(['classroom', 'room', 'venue']), date: idx(['date', 'class date'])
  };
  const errors = [];
  let added = 0, skipped = 0;
  const programs = parsePrograms(state.metaMap);
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    const get = (i) => (i >= 0 && row[i] !== undefined ? String(row[i]).trim() : '');
    let start = normTime(get(ci.start)), end = normTime(get(ci.end));
    if (!start && ci.time >= 0) { const parts = get(ci.time).split(/[-–—]/).map(x => x.trim()); start = normTime(parts[0] || ''); end = normTime(parts[1] || ''); }
    const entry = {
      program: get(ci.program), semester: get(ci.semester).replace(/[^0-9]/g, ''), section: get(ci.section),
      day: get(ci.day), start, end, subject: get(ci.subject), teacher: get(ci.teacher),
      room: get(ci.room), date: normDate(get(ci.date))
    };
    const dayMatch = DAYS.find(d => d.toLowerCase() === entry.day.toLowerCase() || d.slice(0, 3).toLowerCase() === entry.day.toLowerCase());
    if (dayMatch) entry.day = dayMatch;
    if (entry.date && !dayMatch) entry.day = weekdayOf(entry.date);
    if (!entry.date) delete entry.date;
    if (!entry.program || !entry.semester || !entry.day || !entry.start || !entry.subject) {
      skipped++; errors.push('Row ' + (r + 1) + ': missing Program, Semester, Day, Start time or Subject — skipped.');
      continue;
    }
    const prog = programs.find(p => p.name.toLowerCase() === entry.program.toLowerCase());
    if (prog) entry.program = prog.name;
    state.entries.push(entry);
    added++;
  }
  state.entries.forEach((e, i) => { e.id = 'r' + (i + 2); });
  return { added, skipped, errors: errors.slice(0, 20) };
}

/* ---------------- HTTP plumbing ---------------- */
function send(res, code, body, type, extraHeaders) {
  const data = type === 'application/json' ? JSON.stringify(body) : String(body);
  res.writeHead(code, Object.assign({ 'Content-Type': type || 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' }, extraHeaders || {}));
  res.end(data);
}
function readBody(req, limit) {
  return new Promise((resolve) => {
    let b = '';
    const max = limit || 2000000;
    req.on('data', (c) => { b += c; if (b.length > max) req.destroy(); });
    req.on('end', () => { try { resolve(JSON.parse(b || '{}')); } catch (e) { resolve({}); } });
    req.on('error', () => resolve({}));
  });
}
function siblingsOf(state, entry) {
  return state.entries.filter(x => x.day === entry.day && x.start === entry.start && x.subject === entry.subject && (x.teacher || '') === (entry.teacher || ''));
}
async function decide(state, entry, date, action, reason) {
  const sibs = siblingsOf(state, entry);
  const progs = [];
  sibs.forEach(e => { if (!progs.includes(e.program)) progs.push(e.program); });
  const subject = entry.subject;
  state.statusRows = state.statusRows.filter(sr => !(sr.date === date && sr.subject.toLowerCase() === subject.toLowerCase() &&
    (!sr.program || progs.some(p => p.toLowerCase() === sr.program.toLowerCase()))));
  if (action === 'cancel') progs.forEach(p => state.statusRows.unshift({ date, subject, program: p, status: 'Cancelled', reason: reason || '' }));
  if (action === 'confirm') progs.forEach(p => state.statusRows.unshift({ date, subject, program: p, status: 'Confirmed', reason: '' }));
  await saveStatus(state.statusRows);
}
function targetDateForEntry(e) {
  if (!e) return pktNow().date;
  if (e.date) return e.date;
  const today = pktNow().date;
  const names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
  const cur = names.indexOf(weekdayOf(today)), tgt = names.indexOf(e.day);
  if (cur < 0 || tgt < 0) return today;
  const d = new Date(today + 'T12:00:00Z');
  d.setUTCDate(d.getUTCDate() + ((tgt - cur + 7) % 7));
  return d.toISOString().slice(0, 10);
}
function findEntry(state, id) { return state.entries.find(e => e.id === id); }

module.exports = async function handler(req, res) {
  try {
    const u = new URL(req.url, 'http://localhost');
    const p = u.pathname.replace(/\/+$/, '') || '/';
    const method = req.method;

    /* ----- public ----- */
    if (method === 'GET' && p === '/api/timetable') {
      const db = buildDb(await loadState());
      return send(res, 200, {
        department: db.department, university: db.university, programs: db.programs,
        sections: db.sections, subjects: db.subjects, teachers: db.teachers, rooms: db.rooms,
        entries: db.entries, fileMeta: null,
        cancellations: db.cancellations.map(c => ({ entryId: c.entryId, date: c.date, reason: c.reason || '' }))
      }, 'application/json');
    }
    if (method === 'GET' && p === '/api/template.csv') {
      return send(res, 200, 'Program,Semester,Section,Day,Start,End,Subject,Teacher,Classroom\n', 'text/csv');
    }
    if (method === 'GET' && p === '/api/timetable-file') return send(res, 404, 'No timetable file uploaded yet.', 'text/plain');

    /* ----- auth ----- */
    if (method === 'POST' && p === '/api/admin/login') {
      const body = await readBody(req);
      const state = await loadState();
      if (!checkPw(state, body.password || '')) return send(res, 401, { error: 'Incorrect password.' }, 'application/json');
      const secure = (req.headers['x-forwarded-proto'] || 'https') === 'https';
      res.writeHead(200, { 'Content-Type': 'application/json', 'Set-Cookie': 'kiu_admin=' + makeSession() + '; HttpOnly; Path=/; SameSite=Lax' + (secure ? '; Secure' : '') });
      return res.end(JSON.stringify({ ok: true }));
    }
    if (method === 'POST' && p === '/api/admin/forgot-password') {
      const body = await readBody(req);
      const newPw = String(body.newPassword || '');
      if (newPw.length < 6) return send(res, 400, { error: 'New password must be at least 6 characters.' }, 'application/json');
      const state = await loadState();
      if (!checkRec(state, body.recoveryKey || '')) return send(res, 401, { error: 'Incorrect recovery key.' }, 'application/json');
      await saveMeta(state, 'pw', hashSecret(newPw));
      return send(res, 200, { ok: true }, 'application/json');
    }
    if (method === 'POST' && p === '/api/admin/logout') {
      res.writeHead(200, { 'Content-Type': 'application/json', 'Set-Cookie': 'kiu_admin=; HttpOnly; Path=/; Max-Age=0' });
      return res.end(JSON.stringify({ ok: true }));
    }
    if (method === 'GET' && p === '/api/admin/me') {
      return send(res, 200, { admin: isAdmin(req) }, 'application/json');
    }

    /* ----- admin (login required) ----- */
    if (p.startsWith('/api/admin/')) {
      if (!isAdmin(req)) return send(res, 401, { error: 'Admin login required.' }, 'application/json');

      if (method === 'POST' && p === '/api/admin/change-password') {
        const b = await readBody(req);
        const newPw = String(b.newPassword || '');
        if (newPw.length < 6) return send(res, 400, { error: 'New password must be at least 6 characters.' }, 'application/json');
        const state = await loadState();
        if (!checkPw(state, b.currentPassword || '')) return send(res, 401, { error: 'Current password is incorrect.' }, 'application/json');
        await saveMeta(state, 'pw', hashSecret(newPw));
        return send(res, 200, { ok: true }, 'application/json');
      }
      if (method === 'POST' && p === '/api/admin/set-recovery') {
        const b = await readBody(req);
        const key = String(b.recoveryKey || '');
        if (key.length < 6) return send(res, 400, { error: 'Recovery key must be at least 6 characters.' }, 'application/json');
        const state = await loadState();
        await saveMeta(state, 'rec', hashSecret(key));
        return send(res, 200, { ok: true }, 'application/json');
      }
      if (method === 'GET' && p === '/api/admin/data') {
        const db = buildDb(await loadState());
        return send(res, 200, { teacherRecords: db.teacherRecords, cancellations: db.cancellations, confirmations: db.confirmations, sheets: [] }, 'application/json');
      }
      if (method === 'GET' && p === '/api/admin/backup') {
        const state = await loadState();
        const db = buildDb(state);
        const payload = JSON.stringify({ app: 'kiu-media-timetable', version: 1, exportedAt: new Date().toISOString(), db, settings: {} }, null, 2);
        res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Disposition': 'attachment; filename="kiu-media-backup-' + new Date().toISOString().slice(0, 10) + '.json"' });
        return res.end(payload);
      }
      if (method === 'POST' && p === '/api/admin/restore') {
        const b = await readBody(req, 14000000);
        if (!b || !b.db || !Array.isArray(b.db.programs) || !Array.isArray(b.db.entries))
          return send(res, 400, { error: 'This does not look like a KIU Media Timetable backup file.' }, 'application/json');
        const state = await loadState();
        state.entries = b.db.entries.map(e => {
          const x = {
            program: String(e.program || '').trim(), semester: String(e.semester || '').replace(/[^0-9]/g, ''),
            section: String(e.section || '').trim(), day: capDay(e.day), start: normTime(e.start), end: normTime(e.end),
            subject: String(e.subject || '').trim(), teacher: String(e.teacher || '').trim(), room: String(e.room || '').trim()
          };
          const d = normDate(e.date); if (d) x.date = d;
          return x;
        });
        state.entries.forEach((e, i) => { e.id = 'r' + (i + 2); });
        const byId = {};
        (b.db.entries || []).forEach((e, i) => { byId[e.id] = state.entries[i]; });
        state.teacherRecords = (b.db.teacherRecords || []).map(t => ({ name: String(t.name || '').trim(), designation: String(t.designation || '').trim(), email: String(t.email || '').trim() }));
        state.teacherRecords.forEach((t, i) => { t.id = 't' + (i + 2); });
        const rows = [];
        (b.db.cancellations || []).forEach(c => { const e = byId[c.entryId]; if (e) rows.push({ date: c.date, subject: e.subject, program: e.program, status: 'Cancelled', reason: c.reason || '' }); });
        (b.db.confirmations || []).forEach(c => { const e = byId[c.entryId]; if (e && c.status === 'confirmed') rows.push({ date: c.date, subject: e.subject, program: e.program, status: 'Confirmed', reason: '' }); });
        state.statusRows = rows;
        await saveMeta(state, 'programs', JSON.stringify(b.db.programs.map(x => ({ id: x.id || slug(x.name), name: x.name }))));
        await saveTimetable(state.entries);
        await saveTeachers(state.teacherRecords);
        await saveStatus(state.statusRows);
        return send(res, 200, { ok: true }, 'application/json');
      }
      if (p === '/api/admin/upload-file' || p.startsWith('/api/admin/sheets')) {
        return send(res, 404, { error: 'File uploads are not available in this setup.' }, 'application/json');
      }

      /* ----- entries ----- */
      if (method === 'POST' && p === '/api/admin/entries') {
        const b = await readBody(req);
        const state = await loadState();
        const entry = {
          program: String(b.program || '').trim(), semester: String(b.semester || '').trim(),
          section: String(b.section || '').trim(), day: capDay(b.day), start: normTime(b.start), end: normTime(b.end),
          subject: String(b.subject || '').trim(), teacher: String(b.teacher || '').trim(),
          room: String(b.room || '').trim()
        };
        const d = normDate(b.date); if (d) entry.date = d;
        if (!entry.program || !entry.semester || !entry.day || !entry.start || !entry.subject)
          return send(res, 400, { error: 'Program, Semester, Day, Start time and Subject are required.' }, 'application/json');
        state.entries.push(entry);
        entry.id = 'r' + (state.entries.length + 1);
        await saveTimetable(state.entries);
        return send(res, 200, { ok: true, entry }, 'application/json');
      }
      const entryMatch = p.match(/^\/api\/admin\/entries\/(.+)$/);
      if (entryMatch && method === 'PUT') {
        const b = await readBody(req);
        const state = await loadState();
        const e = findEntry(state, decodeURIComponent(entryMatch[1]));
        if (!e) return send(res, 404, { error: 'Entry not found.' }, 'application/json');
        ['program', 'semester', 'section', 'subject', 'teacher', 'room'].forEach(k => { if (b[k] !== undefined) e[k] = String(b[k]).trim(); });
        if (b.day !== undefined) e.day = capDay(b.day);
        if (b.start !== undefined) e.start = normTime(b.start);
        if (b.end !== undefined) e.end = normTime(b.end);
        if (b.date !== undefined) { const d = normDate(b.date); if (d) e.date = d; else delete e.date; }
        await saveTimetable(state.entries);
        return send(res, 200, { ok: true, entry: e }, 'application/json');
      }
      if (entryMatch && method === 'DELETE') {
        const state = await loadState();
        const id = decodeURIComponent(entryMatch[1]);
        state.entries = state.entries.filter(e => e.id !== id);
        state.entries.forEach((e, i) => { e.id = 'r' + (i + 2); });
        await saveTimetable(state.entries);
        return send(res, 200, { ok: true }, 'application/json');
      }

      /* ----- programs ----- */
      if (method === 'POST' && p === '/api/admin/programs') {
        const b = await readBody(req);
        const name = String(b.name || '').trim();
        if (!name) return send(res, 400, { error: 'Program name is required.' }, 'application/json');
        const state = await loadState();
        const programs = parsePrograms(state.metaMap);
        state.entries.forEach(e => { if (e.program && !programs.some(x => x.name === e.program)) programs.push({ id: slug(e.program), name: e.program }); });
        if (programs.some(x => x.name.toLowerCase() === name.toLowerCase()))
          return send(res, 409, { error: 'That program already exists.' }, 'application/json');
        const prog = { id: 'p' + Date.now().toString(36), name };
        programs.push(prog);
        await saveMeta(state, 'programs', JSON.stringify(programs));
        return send(res, 200, { ok: true, program: prog }, 'application/json');
      }
      const progMatch = p.match(/^\/api\/admin\/programs\/(.+)$/);
      if (progMatch && method === 'DELETE') {
        const state = await loadState();
        const programs = parsePrograms(state.metaMap).filter(x => x.id !== decodeURIComponent(progMatch[1]));
        await saveMeta(state, 'programs', JSON.stringify(programs));
        return send(res, 200, { ok: true }, 'application/json');
      }
      if (method === 'POST' && p === '/api/admin/catalog') {
        return send(res, 200, { ok: true }, 'application/json');
      }

      /* ----- teachers ----- */
      if (method === 'POST' && p === '/api/admin/teachers/rename') {
        const b = await readBody(req);
        const from = String(b.from || '').trim(), to = String(b.to || '').trim();
        if (!from || !to) return send(res, 400, { error: 'Both the old and new teacher names are required.' }, 'application/json');
        if (from === to) return send(res, 200, { ok: true, updated: 0 }, 'application/json');
        const state = await loadState();
        const db = buildDb(state);
        if (db.teachers.some(x => x.toLowerCase() === to.toLowerCase()))
          return send(res, 409, { error: 'A teacher with the new name already exists.' }, 'application/json');
        if (!db.teachers.includes(from) && !state.entries.some(e => e.teacher === from))
          return send(res, 404, { error: 'Teacher not found.' }, 'application/json');
        let updated = 0;
        state.entries.forEach(e => { if (e.teacher === from) { e.teacher = to; updated++; } });
        if (updated) await saveTimetable(state.entries);
        return send(res, 200, { ok: true, updated }, 'application/json');
      }
      const tMatch = p.match(/^\/api\/admin\/teachers\/(.+)$/);
      if (tMatch && method === 'DELETE') {
        const name = decodeURIComponent(tMatch[1]);
        const state = await loadState();
        const n = state.entries.filter(e => e.teacher === name).length;
        if (n > 0) return send(res, 409, { error: 'This teacher has ' + n + ' class(es) in the timetable. Use Edit to rename the teacher, or change those classes first.' }, 'application/json');
        return send(res, 200, { ok: true }, 'application/json');
      }
      if (method === 'POST' && p === '/api/admin/teacher-records') {
        const b = await readBody(req);
        const state = await loadState();
        const tr = { name: String(b.name || '').trim(), designation: String(b.designation || '').trim(), email: String(b.email || '').trim() };
        if (!tr.name) return send(res, 400, { error: 'Teacher Name is required.' }, 'application/json');
        if (state.teacherRecords.some(x => x.name.toLowerCase() === tr.name.toLowerCase()))
          return send(res, 409, { error: 'A teacher with this name already exists.' }, 'application/json');
        state.teacherRecords.push(tr);
        tr.id = 't' + (state.teacherRecords.length + 1);
        await saveTeachers(state.teacherRecords);
        return send(res, 200, { ok: true, teacher: tr }, 'application/json');
      }
      const trMatch = p.match(/^\/api\/admin\/teacher-records\/(.+)$/);
      if (trMatch && method === 'PUT') {
        const b = await readBody(req);
        const state = await loadState();
        const tr = state.teacherRecords.find(x => x.id === decodeURIComponent(trMatch[1]));
        if (!tr) return send(res, 404, { error: 'Teacher not found.' }, 'application/json');
        const oldName = tr.name;
        const newName = String(b.name !== undefined ? b.name : oldName).trim();
        if (!newName) return send(res, 400, { error: 'Teacher Name is required.' }, 'application/json');
        if (state.teacherRecords.some(x => x.id !== tr.id && x.name.toLowerCase() === newName.toLowerCase()))
          return send(res, 409, { error: 'Another teacher already has this name.' }, 'application/json');
        ['name', 'designation', 'email'].forEach(k => { if (b[k] !== undefined) tr[k] = String(b[k]).trim(); });
        await saveTeachers(state.teacherRecords);
        if (newName !== oldName) {
          let touched = false;
          state.entries.forEach(e => { if (e.teacher === oldName) { e.teacher = newName; touched = true; } });
          if (touched) await saveTimetable(state.entries);
        }
        return send(res, 200, { ok: true, teacher: tr }, 'application/json');
      }
      if (trMatch && method === 'DELETE') {
        const state = await loadState();
        const i = state.teacherRecords.findIndex(x => x.id === decodeURIComponent(trMatch[1]));
        if (i < 0) return send(res, 404, { error: 'Teacher not found.' }, 'application/json');
        const name = state.teacherRecords[i].name;
        const n = state.entries.filter(e => e.teacher === name).length;
        if (n > 0) return send(res, 409, { error: 'This teacher has ' + n + ' class(es) in the timetable. Change those classes first, then delete.' }, 'application/json');
        state.teacherRecords.splice(i, 1);
        state.teacherRecords.forEach((t, j) => { t.id = 't' + (j + 2); });
        await saveTeachers(state.teacherRecords);
        return send(res, 200, { ok: true }, 'application/json');
      }

      /* ----- import ----- */
      if (method === 'POST' && p === '/api/admin/import') {
        const b = await readBody(req);
        const state = await loadState();
        const result = importCsvInto(state, String(b.csv || ''));
        if (result.added) await saveTimetable(state.entries);
        return send(res, 200, result, 'application/json');
      }

      /* ----- today's decisions ----- */
      if (method === 'POST' && (p === '/api/admin/cancel-class' || p === '/api/admin/confirm-class' || p === '/api/admin/uncancel-class')) {
        const b = await readBody(req);
        const state = await loadState();
        const e = findEntry(state, b.entryId);
        if (!e && p !== '/api/admin/uncancel-class') return send(res, 404, { error: 'Class not found.' }, 'application/json');
        const date = String(b.date || targetDateForEntry(e));
        if (e) {
          if (p === '/api/admin/cancel-class') await decide(state, e, date, 'cancel', String(b.reason || '').trim());
          else if (p === '/api/admin/confirm-class') await decide(state, e, date, 'confirm', '');
          else await decide(state, e, date, 'clear', '');
        }
        return send(res, 200, { ok: true }, 'application/json');
      }

      return send(res, 404, { error: 'Not found.' }, 'application/json');
    }

    return send(res, 404, { error: 'Not found.' }, 'application/json');
  } catch (err) {
    return send(res, 500, { error: 'Server error: ' + (err && err.message ? err.message.slice(0, 200) : 'unknown') }, 'application/json');
  }
};
