--- Выгрузчик против настоящего коллектора.
---
--- Двойник показывает, что мы правильно разговариваем сами с собой.
--- Настоящий коллектор показывает, что нас понимает кто-то ещё: партия
--- принята, а отрезок находится в Tempo по опознавателю — именно
--- отрезок, а не трасса: трассу из одних нулей коллектор принимает
--- с 200, но найти её нечем. Отрицательный контроль — партия с base64
--- в опознавателях получает 400: проверка видит отказ коллектора, а не
--- только его согласие.
---
--- Повтор после потерянного ответа — штатный путь выгрузчика, и что
--- из него увидит оператор, решает хранилище, а не выгрузчик. Tempo
--- сводит копии отрезка с одним `span_id`, только если они лежат
--- в разных записях трассы: повтор, пришедший, пока трасса ещё живая,
--- ложится в ту же запись и виден дважды, как только Tempo закроет
--- трассу, а пришедший после её закрытия — один раз. Метрики отрезков
--- считают копию в обоих случаях. Проверены обе стороны. Живой трассу
--- держат отрезки соседнего узла той же трассы: окно Tempo отсчитывается
--- от последнего отрезка с любого узла, и повтор, которого занятая машина
--- задержала дольше окна стенда, всё равно ложится в живую трассу, а не
--- решает гонку с её закрытием.
---
--- Тело партии по умолчанию сжато gzip, и коллектор читает его
--- по заголовку `Content-Encoding`: партия в 512 отрезков ужимается
--- в разы и доходит до Tempo, несжатая — тоже. Отрицательный контроль —
--- заголовок gzip при несжатом теле получает 400, а сжатое тело без
--- заголовка не принимается: коллектор разжимает то, что названо,
--- и только это.
---
--- Коллектор поднимается отдельно — `test/stand/otel.sh`, — и если его
--- нет, проверки честно пропускаются: гейты не должны зависеть от докера.

local clock = require('clock')
local digest = require('digest')
local fiber = require('fiber')
local t = require('luatest')

local helper = dofile('test/helper.lua')

local g = t.group('tnt.trace.otlp.live')

---@type any
local otlp

---@type any
local trace

---@type any
local http

--- Атрибуты ресурса, с которыми ушла партия; пусто, пока Tempo не ответил.
---@type table
local EMPTY_RESOURCE = { attributes = {} }

--- Где стоит коллектор: те же порты, что поднимает скрипт стенда.
local OTEL = { host = '127.0.0.1', otlp = 14318, tempo = 13200, grafana = 13000 }

--- Адрес коллектора.
local ENDPOINT = ('http://%s:%d'):format(OTEL.host, OTEL.otlp)

--- Адрес Tempo: в нём ищется отправленное.
local TEMPO = ('http://%s:%d'):format(OTEL.host, OTEL.tempo)

--- Адрес Grafana: метрики отрезков читаются через её источник данных,
--- потому что Prometheus стенда наружу не выставлен.
local GRAFANA = ('http://%s:%d'):format(OTEL.host, OTEL.grafana)

--- Запрос к Prometheus стенда через источник данных Grafana.
local PROMETHEUS_QUERY = '/api/datasources/proxy/uid/prometheus/api/v1/query'

--- Сколько ждать появления отрезка в Tempo: на опыте — секунда,
--- запас на занятую машину.
local SEARCH_TIMEOUT = 30

--- Сколько ждать метрик отрезков: генератор метрик Tempo считает раз
--- в 15 с, и Prometheus получает посчитанное не сразу.
local METRICS_TIMEOUT = 60

--- Через сколько секунд Tempo стенда наверняка закрыл трассу, в которую
--- ничего не приходит: образ держит её живой секунду
--- (`ingester.trace_idle_period`) и сверяется раз в секунду
--- (`ingester.flush_check_period`); остальное — запас на занятую машину.
local TEMPO_CLOSED_AFTER = 4

--- Пауза перед повтором в живую трассу. Она дольше такта процессора
--- `batch` у коллектора (200 мс), поэтому копии уходят в Tempo разными
--- обращениями, а не одним. Живой трассу на это время держит сосед
--- (`kept_open`), а не краткость паузы: на занятой машине путь повтора
--- через коллектор бывает длиннее окна стенда.
local WHILE_OPEN = 0.5

--- Как часто сосед шлёт отрезок в трассу, которую держит живой: чаще
--- такта процессора `batch` у коллектора, чтобы каждая его отправка
--- в Tempo несла отрезок этой трассы.
local KEEP_OPEN_EVERY = 0.1

--- Чем слой клиента подменяет потерянный ответ коллектора.
local LOST = 'ответ коллектора потерян'

--- Имя службы в ресурсе: по нему трассы стенда видны в Grafana.
local SERVICE = 'tnt-trace-otlp-live'

--- Сосед, который держит трассу живой (`kept_open`); пусто, пока
--- проверка его не завела.
---@type any
local neighbour

g.before_each(function()
    otlp, trace, http = helper.fresh()

    -- Байты и часы настоящие: опознаватели уникальны, и по ним отрезок
    -- ищется в Tempo среди прочих.
    helper.module('tnt.trace').configure({ record = false })
end)

g.after_each(function()
    -- Сосед останавливается и при упавшей проверке: его файбер шлёт
    -- клиентом из исходников, которые `unload` сейчас уберёт.
    if neighbour ~= nil then
        neighbour.stop()
        neighbour = nil
    end

    helper.unload(otlp)
end)

--- Пропускает проверку, если стенда нет.
local function need_stand()
    t.skip_if(
        not helper.listening(OTEL.host, OTEL.otlp) or not helper.listening(OTEL.host, OTEL.tempo),
        'коллектор не отвечает: test/stand/otel.sh'
    )
end

--- Запускает выгрузку на стенд; проверка пропускается, если стенда нет.
---@param client table|nil Настройки клиента выгрузчика
---@param compression string|nil Сжатие тела партии; пусто — умолчание выгрузчика
local function started(client, compression)
    need_stand()

    t.assert_equals(
        otlp.start({
            endpoint = ENDPOINT,
            service_name = SERVICE,
            interval = 3600,
            client = client,
            compression = compression,
        }),
        true
    )
end

--- Клиент к стенду: без заголовков контекста, чтобы обращения проверки
--- не рождали отрезков в проверяемой трассе.
---@param base_url string
---@return any
local function reader(base_url)
    return (assert(http.new({ base_url = base_url, retry = { attempts = 1 }, propagate = false })))
end

--- Шестнадцатеричная запись опознавателя, который Tempo отдаёт в base64.
local hex_of = helper.hex_of

--- Опрашивает до срока, пока `ready` не отдаст значение, и отдаёт его.
---@param what string Чего ждём — для отказа по сроку
---@param timeout integer Сколько ждать, секунды
---@param ready fun(): any Значение либо пусто, пока его нет
---@return any
local function awaited(what, timeout, ready)
    local deadline = fiber.clock() + timeout

    while true do
        local value = ready()

        if value ~= nil then
            return value
        end

        t.assert_lt(fiber.clock(), deadline, ('%s: не дождались за %d с'):format(what, timeout))
        fiber.sleep(0.5)
    end
end

--- Отрезки трассы из Tempo списком, как их отдал Tempo, — копии одного
--- отрезка не сводятся. Пока трассы нет, список пуст.
---@param tempo any Клиент к Tempo
---@param trace_id string
---@return { spans: table[], resource: table|nil }
local function listed(tempo, trace_id)
    local answer = tempo:get('/api/traces/' .. trace_id)
    local listing = { spans = {} }

    if answer == nil or answer.status ~= 200 then
        return listing
    end

    for _, batch in ipairs(answer:json().batches or {}) do
        listing.resource = listing.resource or batch.resource

        for _, scope in ipairs(batch.scopeSpans or {}) do
            for _, span in ipairs(scope.spans or {}) do
                table.insert(listing.spans, span)
            end
        end
    end

    return listing
end

--- Сколько копий у каждого отрезка: `{ [span_id] = n }`.
---@param spans table[]
---@return table<string, integer>
local function counted(spans)
    local copies = {}

    for _, span in ipairs(spans) do
        local span_id = hex_of(span.spanId)

        copies[span_id] = (copies[span_id] or 0) + 1
    end

    return copies
end

--- Опрашивает Tempo, пока трасса не станет такой, какой её ждут.
---
--- Коллектор отдаёт трассу Tempo не мгновенно, и трасса бывает видна
--- раньше, чем все её отрезки.
---@param trace_id string
---@param ready fun(copies: table<string, integer>): boolean Готова ли трасса — по числу копий каждого отрезка
---@return { spans: table[], resource: table|nil }
local function polled(trace_id, ready)
    local tempo = reader(TEMPO)

    return awaited(('трасса %s в Tempo'):format(trace_id), SEARCH_TIMEOUT, function()
        local listing = listed(tempo, trace_id)

        if ready(counted(listing.spans)) then
            return listing
        end

        return nil
    end)
end

--- Отрезки трассы из Tempo по опознавателю отрезка: `{ [span_id] = span }`.
---
--- Ждутся и названные отрезки, и их общее число: опознавателя
--- клиентского отрезка проверка не знает, но знает, что он есть.
---@param trace_id string
---@param wanted string[] Опознаватели отрезков, которых ждём
---@param total integer Сколько разных отрезков должно быть в трассе
---@return table<string, table> spans
---@return table|nil resource Атрибуты ресурса первой партии
local function found(trace_id, wanted, total)
    local listing = polled(trace_id, function(copies)
        local count = 0

        for _ in pairs(copies) do
            count = count + 1
        end

        for _, span_id in ipairs(wanted) do
            if copies[span_id] == nil then
                return false
            end
        end

        return count >= total
    end)

    local spans = {}

    for _, span in ipairs(listing.spans) do
        spans[hex_of(span.spanId)] = span
    end

    return spans, listing.resource
end

--- Сколько раз поиск TraceQL находит отрезок.
---
--- Отрезок ищется по имени, а опознаватель сверяется в выдаче: Tempo 2.10
--- по `{ span:id = "…" }` не находит отрезок, чей опознаватель
--- начинается с нулевого байта, — каждый 256-й из случайных, и проверка
--- изредка падала бы по сроку без вины выгрузчика. Имя у проверки своё
--- и случайное, поэтому отрезки с ним — копии проверяемого и только они.
---
--- Ждёт, пока найдёт хотя бы `least` раз: поиск видит отрезок чуть позже
--- выдачи трассы.
---@param span_id string Опознаватель отрезка
---@param name string Имя отрезка
---@param least integer
---@return integer
local function searched(span_id, name, least)
    local now = math.floor(clock.time())
    local query = { q = ('{ name = "%s" }'):format(name), start = now - 3600, ['end'] = now + 3600 }
    local tempo = reader(TEMPO)

    return awaited(('поиск отрезка %s в Tempo'):format(span_id), SEARCH_TIMEOUT, function()
        local answer = tempo:get('/api/search', { query = query })

        if answer == nil or answer.status ~= 200 then
            return nil
        end

        local matched = 0

        for _, match in ipairs(answer:json().traces or {}) do
            for _, set in ipairs(match.spanSets or {}) do
                matched = matched + (set.matched or 0)

                for _, span in ipairs(set.spans or {}) do
                    t.assert_equals(
                        span.spanID,
                        span_id,
                        ('поиск по имени %s нашёл чужой отрезок'):format(name)
                    )
                end
            end
        end

        if matched < least then
            return nil
        end

        return matched
    end)
end

--- Сколько вызовов насчитали метрики отрезков Tempo отрезкам с этим именем.
---
--- Ждёт, пока насчитают хотя бы `least`: генератор метрик считает
--- принятое раз в 15 с.
---@param name string
---@param least integer
---@return number
local function calls_of(name, least)
    local query = ('sum(traces_spanmetrics_calls_total{service="%s",span_name="%s"})'):format(SERVICE, name)
    local grafana = reader(GRAFANA)

    return awaited(('метрики отрезка %s'):format(name), METRICS_TIMEOUT, function()
        local answer = grafana:get(PROMETHEUS_QUERY, { query = { query = query } })

        if answer == nil or answer.status ~= 200 then
            return nil
        end

        local series = answer:json().data.result[1]
        local calls = series and tonumber(series.value[2])

        if calls == nil or calls < least then
            return nil
        end

        return calls
    end)
end

--- Значение атрибута OTLP по имени из списка `{ key, value }`.
local attribute = helper.attribute

--- Слой клиента выгрузчика, который теряет первый ответ коллектора.
---
--- Партия доходит до коллектора, и тот её принимает, а выгрузчик ответа
--- не видит — как при обрыве соединения после приёма. Для выгрузчика это
--- сеть, и партия уходит на повтор. Слой помнит тело каждой отправки
--- и код, которым коллектор на неё ответил на самом деле.
---@return table lost `layer` — слой; `bodies` и `statuses` — по порядку отправок
local function losing_first_answer()
    local lost = { bodies = {}, statuses = {} }

    function lost.layer(request, proceed)
        table.insert(lost.bodies, request.body)

        local answer, failure = proceed(request)

        table.insert(lost.statuses, answer and answer.status)

        if #lost.bodies == 1 then
            return nil, LOST
        end

        return answer, failure
    end

    return lost
end

--- Слой клиента выгрузчика, который помнит каждый запрос к коллектору:
--- по нему видно, какие заголовки и какое тело ушли на самом деле.
---@return table seen `layer` — слой; `requests` — запросы по порядку
local function watching()
    local seen = { requests = {} }

    function seen.layer(request, proceed)
        table.insert(seen.requests, request)

        return proceed(request)
    end

    return seen
end

--- Начало тела gzip: два байта сигнатуры RFC 1952.
local GZIP_MAGIC = '\31\139'

--- Отправляет отрезок с атрибутами и находит его в Tempo.
---@param compression string|nil Сжатие тела партии; пусто — умолчание выгрузчика
---@return table request Что ушло коллектору: `headers`, `body`
local function found_in_tempo(compression)
    local seen = watching()

    started({ layers = { seen.layer } }, compression)

    local name = 'live-' .. digest.urandom(4):hex()

    ---@type any
    local current

    trace.within(name, function()
        current = trace.current()
    end, { attributes = { probe = true, rows = 7 } })

    t.assert_equals(otlp.flush(), { { spans = 1, ok = true, status = 200, rejected = 0 } })
    t.assert_equals(otlp.status().spans.accepted, 1)

    local spans, resource = found(current.trace_id, { current.span_id }, 1)
    local span = spans[current.span_id]

    resource = resource or EMPTY_RESOURCE

    t.assert_equals(span.name, name)
    t.assert_equals(span.kind, 'SPAN_KIND_INTERNAL')
    t.assert_equals(hex_of(span.traceId), current.trace_id)
    t.assert_equals(span.parentSpanId, nil)
    t.assert_equals(attribute(span.attributes, 'probe'), true)
    t.assert_equals(attribute(span.attributes, 'rows'), '7')
    t.assert_equals(attribute(resource.attributes, 'service.name'), SERVICE)
    t.assert_equals(#seen.requests, 1)

    return seen.requests[1]
end

--- Отправляет одну и ту же партию дважды, как после потерянного ответа.
---
--- Партия с одним отрезком доходит до коллектора, и тот её принимает, но
--- ответ теряется, и партия ждёт повтора. `before_retry` — что делается
--- до повтора.
---@param before_retry fun(probe: table) Получает отрезок, ушедший в партии
---@return table probe Отрезок, ушедший дважды: `trace_id`, `span_id`
---@return string name Его имя
local function resent(before_retry)
    local lost = losing_first_answer()

    started({ layers = { lost.layer } })

    local name = 'resent-' .. digest.urandom(4):hex()

    ---@type any
    local probe

    trace.within(name, function()
        probe = trace.current()
    end)

    t.assert_equals(otlp.flush(), { { spans = 1, ok = false, retry_in = 1, err = LOST } })

    before_retry(probe)

    t.assert_equals(otlp.flush(), { { spans = 1, ok = true, status = 200, rejected = 0 } })

    -- Коллектору ушла та же партия до байта, и обе копии он принял.
    t.assert_equals(#lost.bodies, 2)
    t.assert_equals(lost.bodies[2], lost.bodies[1])
    t.assert_equals(lost.statuses, { 200, 200 })

    -- Выгрузчик знает о повторе, но не о том, что первая копия дошла:
    -- принятым он считает один отрезок, а коллектор получил два.
    local status = otlp.status()

    t.assert_equals(status.spans.accepted, 1)
    t.assert_equals(status.batches, { sent = 2, retried = 1, discarded = 0 })

    return probe, name
end

--- Держит трассу живой в Tempo: пока не остановят, шлёт коллектору новые
--- отрезки той же трассы, как соседний узел, через который шёл тот же
--- запрос.
---
--- Tempo закрывает трассу, в которую ничего не приходит дольше
--- `ingester.trace_idle_period`, и считает это время от последнего
--- отрезка трассы с любого узла. Шлёт сосед мимо выгрузчика: у того
--- партия ждёт повтора, и новых он не берёт.
---
--- У соседа `sent` — опознаватели посланных отрезков, `send` — послать
--- ещё один и отдать его опознаватель, `stop` — остановить и дождаться.
---@param probe table Отрезок, чью трассу держим: `trace_id`, `span_id`
---@return table keeper
local function kept_open(probe)
    local encode = helper.module('tnt.trace.otlp.encode')
    local collector = reader(ENDPOINT)
    local keeper = { sent = {} }

    ---@type boolean
    local stopped = false

    function keeper.send()
        local now = clock.realtime64()
        local record = helper.record({
            trace_id = probe.trace_id,
            span_id = digest.urandom(8):hex(),
            parent_id = probe.span_id,
            name = 'keep-open',
            kind = 'internal',
            started = now,
            finished = now,
        })

        -- Опознаватель запоминается до отправки: отрезок может лечь
        -- в Tempo раньше, чем коллектор ответит, и выдача трассы,
        -- прочитанная в этот миг, всё равно узнает его соседским.
        keeper.sent[record.span_id] = true

        local body = encode.request({ record }, { ['service.name'] = SERVICE })
        local answer, err = collector:post(otlp.PATH, { json = body })

        -- Строкой, а не отказом luatest: сосед шлёт и из своего файбера,
        -- а отказ luatest — таблица, и `join` отдал бы её без текста.
        if answer == nil or answer.status ~= 200 then
            local reason = answer and answer.status or err

            error(('коллектор не принял отрезок соседа: %s'):format(reason), 0)
        end

        return record.span_id
    end

    local worker = fiber.new(function()
        while not stopped do
            keeper.send()
            fiber.sleep(KEEP_OPEN_EVERY)
        end
    end)

    worker:set_joinable(true)

    function keeper.stop()
        if stopped then
            return
        end

        stopped = true

        -- Сосед, упавший на полпути, трассу не держал, и что бы проверка
        -- ни увидела, это не повтор в живую трассу.
        local ok, err = worker:join()

        t.assert(ok, tostring(err))
    end

    return keeper
end

--- Отрезки трассы без отрезков соседа.
---
--- Каждый отрезок соседа в выдаче ровно один: его партию никто
--- не повторял, и вторая его копия значила бы, что копии в трассе
--- рождает не повтор выгрузчика.
---@param spans table[]
---@param keeper table Сосед из `kept_open`
---@return table[]
local function besides(spans, keeper)
    local own = {}
    local seen = {}

    for _, span in ipairs(spans) do
        local span_id = hex_of(span.spanId)

        if keeper.sent[span_id] then
            local twice = ('отрезок соседа %s в трассе дважды'):format(span_id)

            t.assert_equals(seen[span_id], nil, twice)
            seen[span_id] = true
        else
            table.insert(own, span)
        end
    end

    return own
end

g.test_a_gzipped_batch_is_accepted_and_the_span_is_found_in_tempo_by_its_id = function()
    -- Сжатие — умолчание выгрузчика: партия ушла gzip, и коллектор её
    -- разжал — иначе отрезка в Tempo не было бы.
    local request = found_in_tempo()

    t.assert_equals(request.headers['content-encoding'], 'gzip')
    t.assert_equals(request.body:sub(1, 2), GZIP_MAGIC)
end

g.test_a_batch_without_compression_is_accepted_and_found_too = function()
    local request = found_in_tempo('none')

    t.assert_equals(request.headers['content-encoding'], nil)
    t.assert_equals(request.body:sub(1, 1), '{')
end

g.test_a_full_batch_shrinks_severalfold_and_reaches_tempo = function()
    local seen = watching()

    started({ layers = { seen.layer } })

    -- Полная партия серверных отрезков, как у слоя трассы роутера:
    -- опознаватели настоящие и случайные — их шестнадцатеричная запись
    -- и есть то, что сжимается хуже всего.
    local ROUTES = { '/customers/:id', '/orders', '/orders/:id/items', '/nodes', '/health' }
    local batch = otlp.DEFAULT_BATCH

    ---@type any
    local last

    for index = 1, batch do
        local route = ROUTES[index % #ROUTES + 1] --[[@as string]]

        trace.within(('GET %s'):format(route), function()
            last = trace.current()
        end, {
            kind = 'server',
            attributes = {
                ['http.request.method'] = 'GET',
                ['url.path'] = (route:gsub(':%w+', tostring(index))),
                ['http.route'] = route,
                ['http.response.status_code'] = 200,
            },
        })
    end

    t.assert_equals(otlp.flush(), { { spans = batch, ok = true, status = 200, rejected = 0 } })

    local packed = seen.requests[1].body
    local text = assert(helper.module('tnt.compress').inflate(packed))

    t.assert_equals(seen.requests[1].headers['content-encoding'], 'gzip')

    t.assert_equals(#require('json').decode(text).resourceSpans[1].scopeSpans[1].spans, batch)
    t.assert_lt(
        #packed * 5,
        #text,
        ('партия в %d отрезков: %d байт JSON, сжатая — %d'):format(batch, #text, #packed)
    )

    found(last.trace_id, { last.span_id }, 1)
end

g.test_the_collector_reads_the_body_by_its_content_encoding = function()
    need_stand()

    local encode = helper.module('tnt.trace.otlp.encode')
    local compress = helper.module('tnt.compress')
    local record = helper.record({
        trace_id = digest.urandom(16):hex(),
        span_id = digest.urandom(8):hex(),
        started = clock.realtime64(),
        finished = clock.realtime64(),
    })
    local text = require('json').encode(encode.request({ record }, { ['service.name'] = SERVICE }))
    local client = reader(ENDPOINT)

    --- Отправляет тело с заголовками и отдаёт ответ коллектора.
    ---@param body string
    ---@param encoding string|nil
    ---@return any
    local function posted(body, encoding)
        local headers = { ['content-type'] = 'application/json', ['content-encoding'] = encoding }

        return assert(client:post(otlp.PATH, { body = body, headers = headers }))
    end

    -- Заголовок gzip при несжатом теле: коллектор разжимает по заголовку
    -- и отвечает 400.
    local answer = posted(text, 'gzip')

    t.assert_equals(answer.status, 400)
    t.assert_str_contains(answer.body, 'gzip: invalid header')

    -- Сжатое тело без заголовка коллектор разбирает как JSON и не
    -- принимает: сам он сжатия не угадывает.
    answer = posted(compress.deflate(text), nil)

    t.assert_not_equals(answer.status, 200)

    -- Сжатое с заголовком — принято.
    answer = posted(compress.deflate(text), 'gzip')

    t.assert_equals(answer.status, 200)
    t.assert_equals(answer:json(), { partialSuccess = {} })
end

g.test_a_client_span_of_an_http_attempt_arrives_with_its_server_parent = function()
    started()

    -- Обращение внутри серверного отрезка: клиентский отрезок попытки
    -- даёт крюк, который поставил `start`. Ходим в Tempo — он рядом
    -- и отвечает 200 на `/ready`.
    local client = assert(http.new({ base_url = TEMPO, retry = { attempts = 1 } }))
    local name = 'GET /live-' .. digest.urandom(4):hex()

    ---@type any
    local server

    trace.serve({}, name, function()
        server = trace.current()

        return client:get('/ready')
    end)

    local outcomes = otlp.flush()

    t.assert_equals(#outcomes, 1)
    t.assert_equals(outcomes[1].ok, true)
    t.assert_equals(outcomes[1].spans, 2)

    local records = trace.take(10)

    t.assert_equals(records, {})

    -- Клиентский отрезок — по трассе: его опознаватель ушёл с партией,
    -- и в трассе ждутся оба.
    local spans = found(server.trace_id, { server.span_id }, 2)
    local server_span = spans[server.span_id]

    t.assert_equals(server_span.name, name)
    t.assert_equals(server_span.kind, 'SPAN_KIND_SERVER')

    ---@type table|nil
    local client_span

    for span_id, span in pairs(spans) do
        if span_id ~= server.span_id then
            client_span = span
        end
    end

    t.assert_not_equals(client_span, nil, 'клиентского отрезка нет в трассе')
    ---@cast client_span table
    t.assert_equals(client_span.name, 'HTTP GET')
    t.assert_equals(client_span.kind, 'SPAN_KIND_CLIENT')
    t.assert_equals(hex_of(client_span.parentSpanId), server.span_id)
    t.assert_equals(attribute(client_span.attributes, 'http.request.method'), 'GET')
    t.assert_equals(attribute(client_span.attributes, 'url.path'), '/ready')
    t.assert_equals(attribute(client_span.attributes, 'server.address'), ('%s:%d'):format(OTEL.host, OTEL.tempo))
    t.assert_equals(attribute(client_span.attributes, 'http.response.status_code'), '200')
end

g.test_a_batch_resent_while_tempo_holds_the_trace_open_shows_its_span_twice = function()
    local probe, name = resent(function(first)
        -- Первая копия ушла коллектору, и с этого мига трассу держит
        -- живой сосед: пока он шлёт, Tempo её не закроет, как бы долго
        -- ни шёл повтор.
        neighbour = kept_open(first)
        fiber.sleep(WHILE_OPEN)

        -- Отступ выгрузчика — секунда, а ждать его проверке незачем: что
        -- повтор ляжет в живую трассу, держит сосед, а не срок повтора.
        -- Часы выгрузчика уходят вперёд, и повтор идёт сразу. У Tempo
        -- по умолчанию окно — 5 с, и повтор после обрыва соединения,
        -- через 1–2 с, ложится в него и без соседа.
        otlp._set_source({
            monotonic = function()
                return clock.monotonic() + otlp.MAX_BACKOFF
            end,
        })
    end)

    -- Сам повтор в живой трассе не увидеть: её выдача показывает копии
    -- одной строкой, а поиск TraceQL живых трасс не видит вовсе. Дошёл ли
    -- он, видно по метке — отрезку соседа, посланному после того, как
    -- коллектор принял повтор: коллектор отдаёт Tempo принятое по порядку,
    -- и когда метка в трассе, повтор уже там. Отпущенная трасса живёт ещё
    -- окно — запас на отправку повтора, которую соседняя обогнала.
    local marker = neighbour.send()
    local live = polled(probe.trace_id, function(copies)
        return copies[marker] ~= nil
    end)

    t.assert_equals(counted(besides(live.spans, neighbour)), { [probe.span_id] = 1 })

    -- Повтор в трассе, держать её больше незачем: Tempo закрывает её
    -- одной записью, и копии в ней не сводятся.
    neighbour.stop()

    local listing = polled(probe.trace_id, function(copies)
        return (copies[probe.span_id] or 0) >= 2
    end)
    local own = besides(listing.spans, neighbour)

    t.assert_equals(counted(own), { [probe.span_id] = 2 })

    -- Копии неотличимы ни в чём: тот же опознаватель, имя, миги, род,
    -- атрибуты. Отличить повтор от настоящего отрезка можно только так:
    -- у настоящего свой `span_id`.
    t.assert_equals(own[2], own[1])

    -- Поиск TraceQL находит отрезок тоже дважды.
    t.assert_equals(searched(probe.span_id, name, 2), 2)

    -- И позже копии не сводятся: закрытая трасса уходит в блоки Tempo,
    -- и чтение из них копий одной записи не сводит.
    fiber.sleep(TEMPO_CLOSED_AFTER)

    local closed = listed(reader(TEMPO), probe.trace_id)

    t.assert_equals(counted(besides(closed.spans, neighbour)), { [probe.span_id] = 2 })
end

g.test_a_batch_resent_after_tempo_closed_the_trace_shows_its_span_once = function()
    local probe, name = resent(function(first)
        -- Первая копия уже в Tempo, и трасса стоит без новых отрезков
        -- дольше, чем Tempo держит её живой. Срок повтора тем временем
        -- прошёл по настоящим часам выгрузчика.
        found(first.trace_id, { first.span_id }, 1)
        fiber.sleep(TEMPO_CLOSED_AFTER)
    end)

    -- Метрики — первыми: они показывают, что Tempo получил обе копии.
    -- Генератор метрик считает принятое, а не хранимое, поэтому видит
    -- и копию, которую выдача трассы сведёт. Без этого единица ниже
    -- ничего не доказывала бы: повтор мог просто ещё не дойти.
    t.assert_equals(calls_of(name, 2), 2)

    t.assert_equals(counted(listed(reader(TEMPO), probe.trace_id).spans), { [probe.span_id] = 1 })
    t.assert_equals(searched(probe.span_id, name, 1), 1)
end

g.test_base64_identifiers_are_refused_by_the_collector = function()
    started()

    -- Отрицательный контроль: то же тело, но опознаватели в base64, как
    -- велел бы общий ProtoJSON, — коллектор отвечает 400.
    local encode = helper.module('tnt.trace.otlp.encode')
    local record = helper.record({
        trace_id = digest.base64_encode(digest.urandom(16)),
        span_id = digest.base64_encode(digest.urandom(8)),
        started = clock.realtime64(),
        finished = clock.realtime64(),
    })
    local client = reader(ENDPOINT)

    local answer = client:post(otlp.PATH, { json = encode.request({ record }, { ['service.name'] = SERVICE }) })

    t.assert_equals(answer.status, 400)
    t.assert_str_contains(answer.body, 'length mismatch')

    -- А в шестнадцатеричной записи — принято.
    record = helper.record({ started = record.started, finished = record.finished })
    answer = client:post(otlp.PATH, { json = encode.request({ record }, { ['service.name'] = SERVICE }) })

    t.assert_equals(answer.status, 200)
    t.assert_equals(answer:json(), { partialSuccess = {} })
end
