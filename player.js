// Anime detail page glue: SPA episode switching (no full reload),
// favorites, autoplay-next, resume toast.
(function () {
  var wrap = document.getElementById('player-wrap');
  var grid = document.getElementById('episode-grid');
  var animeId = wrap ? wrap.dataset.animeId : null;

  // ----- Player swap (AJAX, no page reload) ------------------------------

  function buildPlayerNode(data) {
    if (!data || !data.video_url) {
      var miss = document.createElement('div');
      miss.id = 'player-empty';
      miss.className = 'w-full h-full grid place-items-center text-gray-500 text-sm';
      miss.textContent = 'Видео для этого эпизода ещё не загружено.';
      return miss;
    }
    var iframe = document.createElement('iframe');
    iframe.id = 'player-frame';
    iframe.src = '/api/player?episode_id=' + data.episode_id + '&anime_id=' + data.anime_id;
    iframe.dataset.animeId = data.anime_id;
    iframe.dataset.episodeId = data.episode_id;
    iframe.dataset.episodeNumber = data.episode_number;
    iframe.className = 'w-full h-full block';
    iframe.setAttribute('allow', 'autoplay; fullscreen; picture-in-picture; encrypted-media');
    iframe.setAttribute('allowfullscreen', '');
    iframe.setAttribute('referrerpolicy', 'no-referrer');
    return iframe;
  }

  function setActiveBrick(episodeId) {
    if (!grid) return;
    var bricks = grid.querySelectorAll('.episode-brick');
    bricks.forEach(function (b) {
      if (parseInt(b.dataset.episodeId, 10) === episodeId) {
        b.classList.add('is-active');
      } else {
        b.classList.remove('is-active');
      }
    });
  }

  function getOrderedBricks() {
    if (!grid) return [];
    return Array.from(grid.querySelectorAll('.episode-brick'));
  }

  function updateNavButtons(episodeId) {
    var bricks = getOrderedBricks();
    var idx = bricks.findIndex(function (b) {
      return parseInt(b.dataset.episodeId, 10) === episodeId;
    });
    var prev = idx > 0 ? bricks[idx - 1] : null;
    var next = idx >= 0 && idx < bricks.length - 1 ? bricks[idx + 1] : null;

    var prevBtn = document.getElementById('ep-prev');
    var nextBtn = document.getElementById('ep-next');
    if (prevBtn) {
      if (prev) {
        prevBtn.disabled = false;
        prevBtn.dataset.prevId = prev.dataset.episodeId;
        prevBtn.dataset.prevNumber = prev.dataset.episodeNumber;
      } else {
        prevBtn.disabled = true;
      }
    }
    if (nextBtn) {
      if (next) {
        nextBtn.disabled = false;
        nextBtn.dataset.nextId = next.dataset.episodeId;
        nextBtn.dataset.nextNumber = next.dataset.episodeNumber;
      } else {
        nextBtn.disabled = true;
      }
    }
  }

  function updateLabel(data) {
    var numEl = document.querySelector('[data-ep-number]');
    var titleEl = document.querySelector('[data-ep-title]');
    if (numEl) numEl.textContent = data.episode_number;
    if (titleEl) titleEl.textContent = data.title || '';
  }

  function loadEpisode(episodeId, opts) {
    if (!wrap || !episodeId) return;
    opts = opts || {};
    wrap.classList.add('opacity-60');
    fetch('/api/player/data?episode_id=' + episodeId, { credentials: 'same-origin' })
      .then(function (res) {
        if (!res.ok) throw new Error('player data ' + res.status);
        return res.json();
      })
      .then(function (data) {
        // Replace the player node in place — no page reload.
        var existing = wrap.querySelector('iframe, #player-empty');
        var node = buildPlayerNode(data);
        if (existing) {
          wrap.replaceChild(node, existing);
        } else {
          wrap.appendChild(node);
        }
        setActiveBrick(data.episode_id);
        updateNavButtons(data.episode_id);
        updateLabel(data);

        if (!opts.skipHistory && data.page_url) {
          history.pushState(
            { episodeId: data.episode_id, episodeNumber: data.episode_number },
            '',
            data.page_url
          );
        }
      })
      .catch(function (e) {
        console.error('episode load failed', e);
      })
      .then(function () {
        wrap.classList.remove('opacity-60');
      });
  }

  // Brick click → swap player without reload.
  if (grid) {
    grid.addEventListener('click', function (ev) {
      var brick = ev.target.closest('.episode-brick');
      if (!brick) return;
      ev.preventDefault();
      var id = parseInt(brick.dataset.episodeId, 10);
      if (!id) return;
      loadEpisode(id);
    });
  }

  // Prev / next buttons.
  var prevBtn = document.getElementById('ep-prev');
  if (prevBtn) {
    prevBtn.addEventListener('click', function () {
      if (prevBtn.disabled) return;
      var id = parseInt(prevBtn.dataset.prevId, 10);
      if (id) loadEpisode(id);
    });
  }
  var nextBtn = document.getElementById('ep-next');
  if (nextBtn) {
    nextBtn.addEventListener('click', function () {
      if (nextBtn.disabled) return;
      var id = parseInt(nextBtn.dataset.nextId, 10);
      if (id) loadEpisode(id);
    });
  }

  window.addEventListener('popstate', function (ev) {
    var st = ev.state || {};
    if (st.episodeId) {
      loadEpisode(st.episodeId, { skipHistory: true });
    }
  });

  // ----- Autoplay-next from <iframe> postMessage -------------------------
  window.addEventListener('message', function (e) {
    if (!e || !e.data) return;
    if (e.data.type === 'animeflow:ended') {
      var nb = document.getElementById('ep-next');
      if (nb && !nb.disabled) {
        var id = parseInt(nb.dataset.nextId, 10);
        if (id) {
          setTimeout(function () { loadEpisode(id); }, 600);
        }
      }
    }
  });

  // ----- Resume toast wiring ---------------------------------------------
  var toast = document.getElementById('resume-toast');
  if (toast) {
    var resumeAt = parseFloat(toast.dataset.resumeTime || '0');

    requestAnimationFrame(function () {
      toast.classList.remove('translate-y-6', 'opacity-0');
    });

    var dismiss = function () {
      toast.classList.add('translate-y-6', 'opacity-0');
      setTimeout(function () { toast.remove(); }, 320);
    };

    var sendSeek = function (target) {
      var frame = document.getElementById('player-frame');
      if (frame && frame.contentWindow) {
        try {
          frame.contentWindow.postMessage(
            { type: 'animeflow:seek', time: target },
            '*'
          );
        } catch (e) {}
      }
    };

    var yes = document.getElementById('resume-yes');
    if (yes) yes.addEventListener('click', function () { sendSeek(resumeAt); dismiss(); });

    var no = document.getElementById('resume-no');
    if (no) no.addEventListener('click', function () { sendSeek(0); dismiss(); });

    var close = document.getElementById('resume-close');
    if (close) close.addEventListener('click', dismiss);

    setTimeout(dismiss, 12000);
  }

  // ----- Favorite toggle -------------------------------------------------
  var favBtn = document.getElementById('fav-btn');
  if (favBtn) {
    favBtn.addEventListener('click', function () {
      var aId = parseInt(favBtn.dataset.animeId, 10);
      fetch('/api/favorites/toggle', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ anime_id: aId }),
      })
        .then(function (res) {
          if (res.status === 401) { window.location.href = '/login'; return null; }
          return res.json();
        })
        .then(function (data) {
          if (!data) return;
          if (data.favorited) {
            favBtn.classList.add('bg-accent', 'text-white', 'shadow-glow');
            favBtn.classList.remove('bg-surface2', 'text-gray-400');
          } else {
            favBtn.classList.remove('bg-accent', 'text-white', 'shadow-glow');
            favBtn.classList.add('bg-surface2', 'text-gray-400');
          }
        })
        .catch(function (e) { console.error(e); });
    });
  }

  // Initialise nav state on first load (in case server-side prev/next was empty).
  var firstActive = grid ? grid.querySelector('.episode-brick.is-active') : null;
  if (firstActive) {
    updateNavButtons(parseInt(firstActive.dataset.episodeId, 10));
  }

  // ----- Decorate episode bricks with watched/in-progress markers -------
  function applyProgress(items) {
    if (!grid || !items) return;
    var map = {};
    items.forEach(function (it) { map[it.episode_id] = it; });
    grid.querySelectorAll('.episode-brick').forEach(function (b) {
      var id = parseInt(b.dataset.episodeId, 10);
      var info = map[id];
      var bar = b.querySelector('.episode-progress');
      if (!info) {
        b.classList.remove('is-watched');
        b.removeAttribute('data-progress');
        if (bar) bar.style.transform = 'scaleX(0)';
        return;
      }
      if (info.watched) {
        b.classList.add('is-watched');
        b.setAttribute('data-progress', '100');
        if (bar) bar.style.transform = 'scaleX(1)';
      } else if (info.percent > 1) {
        b.classList.remove('is-watched');
        b.setAttribute('data-progress', String(info.percent));
        if (bar) bar.style.transform = 'scaleX(' + (info.percent / 100) + ')';
      }
    });
  }

  function loadProgress() {
    if (!grid || !animeId) return;
    fetch('/api/anime/' + animeId + '/progress', { credentials: 'same-origin' })
      .then(function (r) { return r.ok ? r.json() : { items: [] }; })
      .then(function (data) { applyProgress(data.items || []); })
      .catch(function () {});
  }
  loadProgress();
  // Re-load after each save (debounced via simple interval while page open).
  setInterval(loadProgress, 30000);

  // ----- Admin: re-parse a title with no episodes -----
  var reparseBtn = document.querySelector('[data-anime-reparse]');
  if (reparseBtn) {
    reparseBtn.addEventListener('click', function () {
      var id = reparseBtn.getAttribute('data-anime-reparse');
      reparseBtn.disabled = true;
      reparseBtn.textContent = 'Запускаем парсер…';
      fetch('/admin/anime/' + id + '/reparse', { method: 'POST', credentials: 'same-origin' })
        .then(function (r) {
          if (!r.ok) throw new Error('http ' + r.status);
          return r.json().catch(function () { return {}; });
        })
        .then(function (data) {
          var added = (data && typeof data.episodes_added === 'number') ? data.episodes_added : null;
          reparseBtn.textContent = added !== null
            ? 'Готово: +' + added + ' серий, обновляем…'
            : 'Готово, обновляем…';
          setTimeout(function () { window.location.reload(); }, 1500);
        })
        .catch(function () {
          reparseBtn.disabled = false;
          reparseBtn.textContent = 'Перепарсить тайтл';
        });
    });
  }
})();
