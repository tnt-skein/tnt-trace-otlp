--- Общие средства проверок выгрузчика.
---
--- Трасса, контекст, клиент HTTP, цикл и сжатие — настоящие: выгрузчик
--- проверяется на том же стыке, что и в бою. Двойники — libcurl (двойник
--- транспорта `tnt-http`), часы и имя инстанса.
---
--- Исходники пакета читаются с диска, а не через `require`: у Tarantool
--- свой загрузчик `.rocks`, он идёт раньше `package.path` и подсунул бы
--- установленную копию пакета, если она есть. Проверки тогда шли бы
--- против вчерашнего кода, а покрытие считалось бы по нему.
---
--- Соседи — зависимости пакета и то, что берут они сами, — тоже грузятся
--- файлами, но из `.rocks`, куда их ставит `make deps`, и заново на каждую
--- проверку. Крюки клиента, запись и будильник трассы, двойник транспорта
--- и загрузчик zlib живут в их модулях, и оставленные соседней проверкой
--- они сделали бы порядок проверок частью их смысла. Проверяется этот
--- пакет, а не они: `.rocks` в покрытие не входит.
---
--- Оснастка в `test/testing/` — загрузчик исходников, часы, которые
--- двигает проверка, ловушка журнала и сценарий ответов — грузится так же
--- и один раз на процесс: второй экземпляр загрузчика не знал бы, что
--- вытеснил первый, и не вернул бы вытесненное на место.
---
--- Проверки берут всё через помощник, а не из оснастки напрямую: помощник —
--- единственное, чем файл проверок отличается от того же файла там, где
--- пакет живёт рядом со своими зависимостями.

local digest = require('digest')
local fiber = require('fiber')
local fio = require('fio')
local json = require('json')
local socket = require('socket')
local t = require('luatest')

--- Модули оснастки в порядке зависимостей: ловушка журнала берёт
--- загрузчик.
local TESTING = {
    { name = 'tnt.testing.sources', path = 'test/testing/sources.lua' },
    { name = 'tnt.testing.clock', path = 'test/testing/clock.lua' },
    { name = 'tnt.testing.journal', path = 'test/testing/journal.lua' },
    { name = 'tnt.testing.protocol', path = 'test/testing/protocol.lua' },
}

for _, module in ipairs(TESTING) do
    if package.loaded[module.name] == nil then
        local chunk, failure = loadfile(fio.abspath(module.path))

        if chunk == nil then
            error(('оснастка %s не читается: %s'):format(module.name, tostring(failure)))
        end

        package.loaded[module.name] = chunk()
    end
end

--- Оснастка проверок под теми именами, что зовёт помощник.
local testing = {
    load_sources = package.loaded['tnt.testing.sources'].load,
    unload_sources = package.loaded['tnt.testing.sources'].unload,
    module = package.loaded['tnt.testing.sources'].module,
    clock = package.loaded['tnt.testing.clock'].new,
    capture_log = package.loaded['tnt.testing.journal'].capture,
    script = package.loaded['tnt.testing.protocol'].script,
}

local helper = {}

--- Список модулей по именам: путь складывается из имени от приставки.
---@param prefix string Каталог с модулями: пусто — корень репозитория
---@param names string[] Имена модулей в порядке зависимостей
---@return { name: string, path: string }[]
local function listed(prefix, names)
    local list = {}

    for _, name in ipairs(names) do
        table.insert(list, { name = name, path = prefix .. name:gsub('%.', '/') .. '.lua' })
    end

    return list
end

--- Соседи в порядке зависимостей — из `.rocks`, куда их ставит `make deps`.
---
--- Первым идёт то, что берут, последним — то, что берёт: загруженный
--- позже модуль достался бы только тем, кто грузился после него.
local NEIGHBOURS = listed('.rocks/share/tarantool/', {
    'tnt.external',
    'tnt.clock',
    'tnt.must.fail',
    'tnt.must.types',
    'tnt.must.range',
    'tnt.must.text',
    'tnt.must.choice',
    'tnt.must.cdata',
    'tnt.must.spec',
    'tnt.must',
    'tnt.context.key',
    'tnt.context',
    'tnt.log.plain',
    'tnt.log',
    'tnt.trace.w3c',
    'tnt.trace.queue',
    'tnt.trace.span',
    'tnt.trace.hook',
    'tnt.trace.layer',
    'tnt.trace.message',
    'tnt.trace',
    'tnt.validate.text',
    'tnt.validate.rule',
    'tnt.validate.coerce',
    'tnt.validate.regex',
    'tnt.validate.rules',
    'tnt.validate.formats',
    'tnt.validate.file',
    'tnt.validate.check',
    'tnt.validate.settings',
    'tnt.validate',
    'tnt.retry.attempt',
    'tnt.retry.backoff',
    'tnt.retry.classify',
    'tnt.retry.budget',
    'tnt.retry.breaker',
    'tnt.retry.options',
    'tnt.retry.runner',
    'tnt.retry',
    'tnt.http.url',
    'tnt.http.body',
    'tnt.http.response',
    'tnt.http.failure',
    'tnt.http.policy',
    'tnt.http.settings',
    'tnt.http.transport',
    'tnt.http.stream',
    'tnt.http.hook',
    'tnt.http',
    'tnt.id.octets',
    'tnt.id.quote',
    'tnt.id.crockford',
    'tnt.id.entropy',
    'tnt.id.sequence',
    'tnt.id.uuid7',
    'tnt.id.ulid',
    'tnt.id',
    'tnt.loop',
    'tnt.compress.system',
    'tnt.compress.zlib',
    'tnt.compress.failure',
    'tnt.compress.stream',
    'tnt.compress.deflater',
    'tnt.compress.inflater',
    'tnt.compress.layer',
    'tnt.compress',
})

--- Модули в порядке зависимостей: соседи, затем исходники самого пакета.
helper.MODULES = {}

for _, list in ipairs({
    NEIGHBOURS,
    listed('', { 'tnt.trace.otlp.encode', 'tnt.trace.otlp.reply', 'tnt.trace.otlp.settings', 'tnt.trace.otlp' }),
}) do
    for _, module in ipairs(list) do
        table.insert(helper.MODULES, module)
    end
end

--- Уже загруженный модуль: пакета либо соседа из той же загрузки.
helper.module = testing.module

--- Этот файл — так его называет место в сообщении об ошибке.
local THIS_FILE = assert(debug.getinfo(1, 'S'), 'нет отладочной информации').short_src

--- Текст исключения от вызова и место, на которое оно должно указывать.
---
--- Вызов идёт из замыкания здесь, а не прямо из `pcall`: `error(msg, 2)`
--- упирается в C-кадр `pcall`, о котором Lua сказать нечего, и место
--- в сообщении пропадает. Вызов нарочно не хвостовой: хвостовой кадр
--- LuaJIT снимает со стека, и вина съехала бы ещё на кадр. Поэтому
--- «строка вызывающего» — строка этого замыкания, и проверка сверяет
--- с ней: так видно, что вина легла на того, кто позвал фасад, а не
--- внутрь пакета.
---@param fn function
---@param ... any
---@return string err
---@return string place `файл:строка: ` вызова
function helper.raised(fn, ...)
    local call = { n = select('#', ...), ... }
    local line
    local ok, err = pcall(function()
        line = assert(debug.getinfo(1, 'l')).currentline + 1
        fn(unpack(call, 1, call.n))
    end)

    t.assert_equals(ok, false, 'вызов должен был бросить')

    return tostring(err), ('%s:%d: '):format(THIS_FILE, line)
end

--- Негодный аргумент — нарочно: анализатор типов о намерении проверки
--- не знает и справедливо ругался бы на негодное значение в вызове.
---@param value any
---@return any
function helper.wrong(value)
    return value
end

--- Адрес коллектора в проверках.
helper.ENDPOINT = 'http://collector.example.org:4318'

--- Куда уходит партия.
helper.TRACES_URL = helper.ENDPOINT .. '/v1/traces'

--- Настройки проверки поверх умолчаний: новая таблица, умолчания целы.
---@param defaults table
---@param overrides table|nil
---@return table
local function merged(defaults, overrides)
    local result = {}

    for _, given in ipairs({ defaults, overrides or {} }) do
        for name, value in pairs(given) do
            result[name] = value
        end
    end

    return result
end

--- Загружает исходники заново и отдаёт названный модуль.
---
--- Для проверок частей — кодировщика, приговора: им выгрузчик целиком
--- не нужен, но части берут соседей, и грузятся все вместе.
---@param name string Какой модуль отдать: tnt.trace.otlp.encode…
---@return any
function helper.load(name)
    return testing.load_sources(helper.MODULES, name)
end

--- Убирает исходники: следующая проверка грузит их заново.
function helper.forget()
    testing.unload_sources(helper.MODULES)
end

--- Загружает исходники заново и отдаёт выгрузчик, трассу и клиент.
---
--- Заново на каждую проверку: настройки, клиент, ждущая партия и счётчики
--- живут в модуле, и оставленные соседней проверкой они сделали бы порядок
--- проверок частью их смысла.
---@return any otlp
---@return any trace
---@return any http
function helper.fresh()
    local otlp = helper.load('tnt.trace.otlp')

    return otlp, testing.module('tnt.trace'), testing.module('tnt.http')
end

--- Останавливает выгрузку, снимает двойники и крюки трассы с очередей
--- процесса и убирает исходники: следующая проверка грузит их заново.
---@param otlp any
function helper.unload(otlp)
    otlp.stop()
    testing.module('tnt.http.transport')._set_source(nil)
    testing.module('tnt.retry.runner')._set_source(nil)
    helper.forget_message_hooks(testing.module('tnt.trace'))
    helper.forget()
end

--- Снимает крюк трассы с загруженных очередей договора.
---
--- Список крюков очереди — состояние процесса, а выгрузчик ставит крюк
--- каждой загруженной очереди. Оставленный проверкой, он достался бы
--- следующим проверкам вместе с трассой, которую проверка уже выгрузила.
---@param trace any Фасад трассы, чьи крюки снимаются
function helper.forget_message_hooks(trace)
    for key in pairs(trace.message_hooks) do
        local queue = package.loaded[trace.MODULES[key]]

        if queue ~= nil then
            queue.hook('tnt.trace', nil)
        end
    end
end

--- Имена модулей очередей договора: выгрузчик спрашивает их загруженными.
local QUEUES = { 'tnt.queue', 'tnt.event', 'tnt.amqp', 'tnt.kafka' }

--- Исполняет `fn`, пока загруженными числятся только названные очереди
--- договора, и возвращает прежние на место в любом исходе.
---@param queues table<string, table> Имя модуля → двойник
---@param fn function
function helper.with_loaded_queues(queues, fn)
    local real = {}

    for _, name in ipairs(QUEUES) do
        real[name] = package.loaded[name]
        package.loaded[name] = queues[name]
    end

    local ok, err = pcall(fn)

    for _, name in ipairs(QUEUES) do
        package.loaded[name] = real[name]
    end

    t.assert_equals(ok, true, tostring(err))
end

--- Двойник модуля очереди договора: помнит поставленные крюки.
---@return table queue
function helper.fake_queue()
    local hooks = {}
    local order = {}
    local queue = {}

    function queue.hook(name, hook)
        for index, known in ipairs(order) do
            if known == name then
                table.remove(order, index)
            end
        end

        if hook ~= nil then
            table.insert(order, name)
        end

        hooks[name] = hook
    end

    function queue.hooks()
        return order
    end

    function queue.get(name)
        return hooks[name]
    end

    return queue
end

--- Выгрузчик на двойниках часов, трасса на запись выключена и ловушка
--- журнала — всё, с чем работает проверка выгрузчика.
---
--- Часы двойника одни у трассы и у выгрузчика: срок повтора и отрезки
--- меряются одним временем.
---@return table world `otlp`, `trace`, `http`, `journal`, `clock`
function helper.world()
    local otlp, trace, http = helper.fresh()
    local world = { otlp = otlp, trace = trace, http = http, journal = testing.capture_log() }

    world.clock = helper.recording(trace, { record = false })
    otlp._set_source({ monotonic = world.clock.monotonic })

    return world
end

--- Отпускает журнал и убирает всё, что собрал `world`.
---@param world table
function helper.release(world)
    world.journal.release()
    helper.unload(world.otlp)
end

--- Группа проверок выгрузчика: мир собирается перед каждой проверкой
--- и убирается после.
---
--- Таблица мира одна на группу и заполняется заново: проверки читают
--- из неё поля в миг вызова, а не ссылки, взятые при загрузке файла.
---@param name string
---@return table g Группа luatest
---@return table world `otlp`, `trace`, `http`, `journal`, `clock` текущей проверки
function helper.group(name)
    local g = t.group(name)
    local world = {}

    g.before_each(function()
        for key, value in pairs(helper.world()) do
            world[key] = value
        end
    end)

    g.after_each(function()
        helper.release(world)
    end)

    return g, world
end

--- Двойник libcurl с ответами по порядку; ставится до `start`: клиент
--- забирает обработчик при сборке.
---
--- Двойник помнит, о чём его просили, и отдаёт заранее написанные ответы
--- по порядку. Настоящий сокет для этого не нужен, а вот порядок
--- обращений — нужен весь: повторы только по нему и видны.
---@param script table[]
---@return table sent Что уходило коллектору
function helper.serving(script)
    local sent = {}
    local answers = testing.script(script)

    local handle = {
        request = function(_, method, url, body, options)
            table.insert(sent, { method = method, url = url, body = body, options = options })

            -- Кончившийся сценарий — ошибка проверки: клиент, сходивший
            -- к серверу лишний раз, обязан об этом сказать.
            return answers.next()
        end,
    }

    testing.module('tnt.http.transport')._set_source({
        client = function()
            return handle
        end,
    })

    return sent
end

--- Запускает выгрузку с двойником коллектора и отдаёт, что ему ушло.
---@param otlp any
---@param script table[] Ответы коллектора по порядку
---@param overrides table|nil Настройки старта поверх умолчаний проверки
---@return table sent
function helper.started(otlp, script, overrides)
    local sent = helper.serving(script)

    t.assert_equals(otlp.start(helper.settings(overrides)), true)

    return sent
end

--- Ответы коллектора: столько партий принято.
---@param count integer
---@return table[]
function helper.accepting(count)
    local script = {}

    for _ = 1, count do
        table.insert(script, helper.accepted())
    end

    return script
end

--- Ответ коллектора на принятую партию.
---@param overrides table|nil
---@return table
function helper.accepted(overrides)
    return helper.answer(200, merged({ body = '{"partialSuccess":{}}' }, overrides))
end

--- Ответ коллектора с кодом и телом — такой, какой отдал бы libcurl.
---
--- Заголовки приходят как есть, в том числе с заглавными буквами: чужой
--- сервер пишет их как вздумается, и приведение к нижнему регистру —
--- работа клиента, а не проверки.
---@param status integer
---@param overrides table|nil Поля поверх умолчаний
---@return table
function helper.answer(status, overrides)
    return merged({ status = status, reason = 'Ok', headers = {}, body = '' }, overrides)
end

--- Ответа нет: так `http.client` сообщает об отказе сети до отправки —
--- имя не разрешилось, соединение не открылось. Заголовков нет вовсе,
--- и это единственная надёжная примета такого отказа.
---@param reason string|nil Что сказал libcurl
---@return table
function helper.no_answer(reason)
    return { status = 595, reason = reason or 'Could not resolve hostname' }
end

--- Слой клиента, который держит отправку, пока проверка не отпустит.
---
--- Двойник транспорта отвечает без уступки управления, а одновременные
--- отправки видны только тогда, когда первая стоит в пути. Слой — это
--- настройка `tnt-http` (`client.layers`), и выгрузчик отдаёт её клиенту
--- как есть.
---@return table hold `layer` — слой; `close`/`open` — держать и отпустить; `waiting` — сколько стоит
function helper.holding()
    local hold = { waiting = 0, closed = false }
    local opened = fiber.cond()

    function hold.layer(request, proceed)
        while hold.closed do
            hold.waiting = hold.waiting + 1
            opened:wait()
            hold.waiting = hold.waiting - 1
        end

        return proceed(request)
    end

    function hold.close()
        hold.closed = true
    end

    function hold.open()
        hold.closed = false
        opened:broadcast()
    end

    return hold
end

--- Запускает функцию в своём файбере и отдаёт его: дождаться — `join`.
---
--- Файбер успевает дойти до первой уступки: у одновременной отправки
--- к этому мигу видно, где она стоит.
---@param fn function
---@return table
function helper.spawned(fn)
    local worker = fiber.new(fn)

    worker:set_joinable(true)
    fiber.yield()

    return worker
end

--- Обращение под номером; его отсутствие — ошибка самой проверки.
---@param sent table[]
---@param index integer
---@return table
function helper.at(sent, index)
    return (assert(sent[index], ('запроса №%d не было'):format(index)))
end

--- Наносекунд в секунде — целым int64, чтобы произведение не теряло точности.
local NANOS = 1000000000LL

--- Секунды стенных часов двойника — в наносекунды int64.
---
--- Целая часть умножается уже в int64, дробная — отдельно: секунды
--- с дробью, умноженные в double, теряют наносекунды выше 2⁵³.
---@param seconds number
---@return any Наносекунды int64
function helper.nanos(seconds)
    local whole = math.floor(seconds)

    return NANOS * whole + math.floor((seconds - whole) * 1000000000 + 0.5)
end

--- Отвечает ли кто-нибудь на этом порту: живые проверки без стенда
--- пропускаются, а не падают.
---@param host string
---@param port integer
---@return boolean
function helper.listening(host, port)
    local connection = socket.tcp_connect(host, port, 0.3)

    if connection == nil then
        return false
    end

    connection:close()

    return true
end

--- Шестнадцатеричная запись опознавателя, который Tempo отдаёт в base64.
---@param encoded string|nil
---@return string|nil
function helper.hex_of(encoded)
    if encoded == nil then
        return nil
    end

    return digest.base64_decode(encoded):hex()
end

--- Значение атрибута OTLP по имени из списка `{ key, value }`, как его
--- отдаёт Tempo.
---@param attributes table[]|nil
---@param key string
---@return any
function helper.attribute(attributes, key)
    for _, entry in ipairs(attributes or {}) do
        if entry.key == key then
            local _, value = next(entry.value)

            return value
        end
    end

    return nil
end

--- Запись закрытого отрезка для кодировщика: разумные умолчания и поля
--- проверки поверх.
---@param overrides table|nil
---@return TntTraceRecord
function helper.record(overrides)
    local record = {
        trace_id = ('11'):rep(16),
        span_id = ('22'):rep(8),
        parent_id = nil,
        name = 'GET /customers/:id',
        kind = 'server',
        trace_flags = '03',
        tracestate = nil,
        started = helper.nanos(1700000000),
        finished = helper.nanos(1700000000.25),
        duration = 0.25,
        status = 'unset',
        error = nil,
        attributes = {},
    }

    for name, value in pairs(overrides or {}) do
        record[name] = value
    end

    return record
end

--- Источник байтов, отдающий на каждый спрос одно и то же: трассе —
--- `0x11` шестнадцать раз, отрезку — `0x22` восемь.
---
--- Отрезки нужны как содержимое партий, и различаются ли их
--- опознаватели, проверкам выгрузчика не важно.
---@return fun(count: integer): string
local function same_bytes()
    return function(count)
        if count == 16 then
            return string.char(0x11):rep(16)
        end

        return string.char(0x22):rep(count)
    end
end

--- Настраивает трассу на запись с известными байтами и часами двойника.
---
--- Отрезки нужны как содержимое партий, а не сами по себе: байты одни
--- и те же на каждый отрезок, часы стоят. Стенные часы — наносекундами
--- int64 от `wall` двойника: так конец отрезка сверяется целым числом.
---@param trace any
---@param opts table|nil Настройки трассы поверх умолчаний проверки
---@return TntTestingClock
function helper.recording(trace, opts)
    trace.configure(merged({ random = same_bytes(), record = true }, opts))

    local clock = testing.clock()

    trace._set_source({
        monotonic = clock.monotonic,
        realtime64 = function()
            return helper.nanos(clock.wall)
        end,
    })

    return clock
end

--- Записывает столько закрытых отрезков, по одному имени на каждый.
---@param trace any
---@param count integer
---@return string[] names
function helper.spans(trace, count)
    local names = {}

    for index = 1, count do
        local name = ('span-%d'):format(index)

        trace.within(name, function() end)
        table.insert(names, name)
    end

    return names
end

--- Настройки старта поверх умолчаний проверки.
---@param overrides table|nil
---@return table
function helper.settings(overrides)
    return merged({ endpoint = helper.ENDPOINT, service_name = 'panel', interval = 3600 }, overrides)
end

--- Тело отправленной партии строкой JSON: сжатое gzip — разжатым.
---
--- Разжимает тот, кто написан заголовком: тело, сжатое без заголовка,
--- осталось бы сжатым и не разобралось бы — проверка это увидит.
---@param sent table[]
---@param index integer
---@return string
function helper.text_of(sent, index)
    local call = helper.at(sent, index)

    if call.options.headers['content-encoding'] == 'gzip' then
        return assert(testing.module('tnt.compress').inflate(call.body))
    end

    return call.body
end

--- Тело отправленной партии, разобранное из JSON.
---@param sent table[]
---@param index integer
---@return table
function helper.body_of(sent, index)
    return json.decode(helper.text_of(sent, index))
end

--- Записи журнала уровня `warn` по порядку.
---@param journal TntTestingJournal
---@return table[]
function helper.warnings(journal)
    local warnings = {}

    for _, entry in ipairs(journal.records()) do
        if entry.level == 'warn' then
            table.insert(warnings, entry)
        end
    end

    return warnings
end

return helper
