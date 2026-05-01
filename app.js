// Mobile menu
function openMobile() {
  document.getElementById('mobile-overlay').classList.remove('hidden');
  const sb = document.getElementById('mobile-sidebar');
  sb.classList.remove('translate-x-full');
}
function closeMobile() {
  document.getElementById('mobile-overlay').classList.add('hidden');
  document.getElementById('mobile-sidebar').classList.add('translate-x-full');
}
window.openMobile = openMobile;
window.closeMobile = closeMobile;

// Live search
(function () {
  const input = document.getElementById('search-input');
  const results = document.getElementById('search-results');
  if (!input || !results) return;

  let t = null;
  let lastQuery = '';

  function hide() {
    results.classList.add('hidden');
    results.innerHTML = '';
  }
  function show(html) {
    results.innerHTML = html;
    results.classList.remove('hidden');
  }

  async function run(q) {
    try {
      const res = await fetch(`/api/search?q=${encodeURIComponent(q)}`);
      const data = await res.json();
      if (q !== lastQuery) return;
      if (!data.results || data.results.length === 0) {
        show(`<div class="px-4 py-3 text-sm text-gray-500">Ничего не найдено</div>`);
        return;
      }
      const html = data.results
        .map(
          (a) => `
          <a href="/anime/${a.id}" class="flex items-center gap-3 px-3 py-2 hover:bg-white/5 transition">
            <div class="w-10 h-14 rounded-md overflow-hidden bg-surface2 shrink-0">
              ${a.poster_url ? `<img src="${a.poster_url}" class="w-full h-full object-cover" />` : ''}
            </div>
            <div class="min-w-0">
              <div class="text-sm font-semibold truncate">${escapeHtml(a.title)}</div>
              <div class="text-xs text-gray-500">${a.year || ''}</div>
            </div>
          </a>`
        )
        .join('');
      show(html);
    } catch (e) {
      hide();
    }
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }

  input.addEventListener('input', (e) => {
    const q = e.target.value.trim();
    lastQuery = q;
    clearTimeout(t);
    if (q.length < 1) {
      hide();
      return;
    }
    t = setTimeout(() => run(q), 180);
  });

  document.addEventListener('click', (e) => {
    const wrap = document.getElementById('search-wrap');
    if (wrap && !wrap.contains(e.target)) hide();
  });

  input.addEventListener('focus', () => {
    if (input.value.trim().length > 0 && results.innerHTML) {
      results.classList.remove('hidden');
    }
  });
})();

// Footer year
document.querySelectorAll('footer').forEach((f) => {
  f.innerHTML = f.innerHTML.replace('{{ now_year }}', new Date().getFullYear());
});
