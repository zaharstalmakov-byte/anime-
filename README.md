# ANIMEFLOW

Anime streaming web app — FastAPI + SQLite + Jinja2 + Tailwind (CDN) + Plyr.

## Run

```bash
cd artifacts/api-server
PORT=${PORT:-8080} SESSION_SECRET=dev-secret \
  python -m uvicorn main:app --host 0.0.0.0 --port $PORT
```

The DB is auto-created on first start at `data/animeflow.db` and seeded with 5 anime, 3 episodes each.

## Routes

- `/` — каталог аниме
- `/anime/{id}` — страница аниме (player + episodes)
- `/anime/{id}/episode/{n}` — конкретный эпизод
- `/search?q=` — поиск
- `/login`, `/register`, `/logout`, `/profile`
- `/top`, `/schedule`, `/random`, `/admin`
- `/api/healthz`, `/api/search?q=`, `/api/favorites/toggle`, `/api/progress`

## Парсер: YummyAnime → Anilibria → Animedia (каскад источников)

Все три источника опрашиваются **параллельно** через резидентный прокси
для каждого тайтла (см. ниже про прокси). Приоритет на коллизии номеров:
**YummyAnime > Anilibria > Animedia**.

* **YummyAnime** (`yummyani.me`) — основной источник серий. Парсер
  сначала ищет полный список эпизодов здесь.
* **Anilibria** (`anilibria.top`) — заполняет номера, которых не было на
  YummyAnime, плюс используется как seed для каталога (какие тайтлы
  вообще брать).
* **Animedia** (`online.animedia.tv`, `animedia.tv`) — последний резерв,
  достаёт оставшиеся номера, если их не было ни на YummyAnime, ни на
  Anilibria.
* Дубликатов не возникает — каждый номер серии берётся ровно с одного
  источника, того, который первый по приоритету его вернул.
* Все нестыковки фиксируются в логах админки: пропуски в нумерации,
  отфильтрованные дубли, поэпизодная разбивка по источникам.

## Резидентный прокси (Geonode)

Каждый исходящий запрос парсера (YummyAnime, Anilibria, Animedia,
Shikimori) идёт через резидентный прокси, чтобы исходные сайты не
блокировали IP контейнера Replit.

По умолчанию используется Geonode-шлюз с учётными данными из брифа,
но всё переопределяется без правок кода через переменные окружения:

| Переменная               | Назначение                                                   | Дефолт                                  |
|--------------------------|--------------------------------------------------------------|-----------------------------------------|
| `PARSER_PROXY_DISABLED`  | `1` / `true` — полностью обойти прокси                      | (не задан)                              |
| `PARSER_PROXY_URL`       | Полный URL `http://user:pass@host:port`, перекрывает остальное | (не задан)                              |
| `PARSER_PROXY_USER`      | Имя пользователя прокси                                     | `geonode_SgbsncVlMl`                    |
| `PARSER_PROXY_PASS`      | Пароль прокси                                               | (значение из брифа)                     |
| `PARSER_PROXY_HOST`      | Хост прокси                                                 | `premium-residential.geonode.com`       |
| `PARSER_PROXY_PORT`      | Порт прокси                                                 | `9000`                                  |

Каждое сообщение в логе админки начинается с пометки `proxy: user@host:port`,
чтобы было видно, через что сейчас работает парсер. Сетевые/прокси-ошибки
(`RemoteProtocolError`, `ConnectError`, `ProxyError`, `TimeoutException`,
`ConnectionError`) ловятся отдельно и пишутся как `WARN: прокси/сеть —
…`, а не как фатальный сбой парсера.

### MAL и AniList

MAL и AniList **не используются** для основного потока серий и стримов.
В этой сборке они зарезервированы только под:

* субтитры (если основной источник — без сабов);
* серии с не-русской озвучкой, когда русской дорожки нигде нет.

Метаданные (постер, рейтинг, описание, жанры) подгружаются с **Shikimori**
исключительно как обогащение карточки.

### Админ-эндпоинты

* `POST /admin/anime/{id}/reparse` — полный re-parse одного тайтла
  (Anilibria → YummyAnime → Animedia) со стиранием старых серий и без
  дубликатов. Кнопка «Перепарсить» в админке вызывает именно это.
* `POST /admin/parser/run?anime_id={id}` — то же самое, но через общий
  эндпоинт запуска парсера, с фильтром по тайтлу.
* `POST /admin/parser/run` без параметров — массовый парсинг всего
  каталога Anilibria, к каждому тайтлу применяется YummyAnime- и
  Animedia-fallback.
* `GET /admin/parser/status?after=...` и WebSocket `/admin/parser/ws` —
  живые логи с пометкой источника на каждой серии.

### Авто-обновление

Раз в 30 минут фоновый цикл парсера проходит по последним 100 тайтлам и
докачивает новые серии с тех же трёх источников в том же порядке
приоритетов. Идущие сейчас сериалы остаются актуальными без ручного
re-parse.
