--- Выгрузка трасс `tnt-trace` в коллектор OpenTelemetry по OTLP/HTTP.
---
--- `tnt-trace` складывает закрытые записанные отрезки в очередь с потолком
--- и больше ничего не знает. Этот пакет — выгрузчик: фоновый цикл
--- на `tnt-loop` забирает партии `trace.take(n)` и шлёт их коллектору
--- в JSON. Как пользоваться — `docs/trace-otlp.md`.
---
---     local otlp = require('tnt.trace.otlp')
---
---     otlp.start({ endpoint = 'http://otel-collector:4318', service_name = 'panel' })
---     otlp.status().running            --> true
---     otlp.stop()
---
--- **Отдельный пакет, а не часть `tnt-trace`.** Выгрузчику нужны
--- `tnt-http` и `tnt-loop`, а `tnt-trace` берут все, кому нужен лишь
--- `trace_id` в журнале, — тянуть за ним клиент HTTP с его повторами
--- значило бы ставить его каждому. Пакет, законно зависящий и от трассы,
--- и от клиента, — естественное место, где крюки трассы ставятся сами:
--- `start` зовёт `trace.install`, и приложение получает клиентские отрезки
--- и отрезки сообщений без единой своей строки. Очереди договора
--- (`tnt-queue`, `tnt-event`, `tnt-amqp`, `tnt-kafka`) спрашиваются
--- по имени и только загруженные: зависимости от них нет, а незагруженной
--- очереди крюк ставить некуда.
---
--- **Клиент выгрузчика собирается явно**, и шесть его настроек задаёт сам
--- выгрузчик: в `client` их не передают, такая попытка бросает, а не
--- теряется молча. `base_url`, `headers` и `timeout` приходят настройками
--- старта `endpoint`, `headers`, `timeout`; три прочие не меняются вовсе:
--- `propagate = false` — отправка партии, случись она в чьей-то области,
--- иначе завела бы клиентский отрезок о самой себе, и тот попал бы
--- в следующую партию; `retry = { attempts = 1 }` — повторы ведёт
--- выгрузчик, иначе два цикла перемножились бы; `max_redirects = 0` —
--- коллектор никуда не переезжает. Умолчание срока — 5 с.
---
--- **Узел выгрузки не ждёт никогда.** Отрезок кладётся в очередь
--- `tnt-trace` без уступки, а сверх потолка новый отбрасывается; партия
--- уходит из фонового файбера. Партия — до 512 отрезков, такт — секунда
--- либо полная партия: набравшаяся партия будит цикл будильником очереди
--- (`trace.on_filled`), и полные партии уходят подряд, не дожидаясь такта.
---
--- **Отправка одна за раз.** Такт цикла и ручной `flush` ждут друг друга:
--- вместе они поделили бы ждущую партию — отправили бы её дважды, а их
--- неудачи затёрли бы одна другую, и партия пропала бы без счёта.
--- Остановка и перенастройка посреди отправки дают ей дослать партию,
--- которая уже в пути, но следующей она не берёт: клиент уже не тот.
---
--- **Ответы коллектора — по OTLP** (`tnt.trace.otlp.reply`): 2xx —
--- принята, `partialSuccess.rejectedSpans` считается отвергнутым; 429, 502,
--- 503, 504 и сеть — повтор той же партии с отступом по степени двойки
--- (1, 2, 4, … до 30 с), не больше `retries` повторов (умолчание 5); пока
--- партия ждёт, новые отрезки копятся в очереди до потолка. `Retry-After`
--- читается как у `tnt.retry`: повтор не раньше названного, а просьба
--- ждать дольше потолка выбрасывает партию — прийти раньше значит сделать
--- то, о чём просили не делать, а ждать дольше — держать очередь без
--- выгрузки по чужому слову. Прочие отказы — партия выбрасывается
--- и считается.
---
--- **Тело партии сжимается gzip** (`Content-Encoding: gzip`) — по умолчанию:
--- JSON партии с шестнадцатеричными опознавателями ужимается почти в девять
--- раз, а сеть узла и коллектора одна на всех. Сжимает `tnt-compress` на третьем уровне: сжатие идёт
--- в потоке событий узла, и третий уровень — лучший из быстрых, вдвое
--- дешевле шестого при почти том же выходе. `compression = 'none'` шлёт
--- как есть — для коллектора, который gzip не читает. Узел без zlib
--- узнаёт об этом отказом `start`, а не броском на каждом такте.
---
--- **Ресурс** — `service.name` из настройки, `service.instance.id` — имя
--- инстанса из `box.cfg`, если узел настроен, и `resource` из настройки
--- сверх того; `service.instance.id` из `resource` главнее имени узла.
--- Умолчание адреса — `http://127.0.0.1:4318`; путь `/v1/traces`
--- дописывается к нему, и путь самого адреса сохраняется приставкой.
---
--- **Очередь `tnt-trace` снимает кто-то один**: этот выгрузчик либо
--- приёмник Sentry с отрезками (модуль `tnt.sentry`). Двое забирали бы
--- друг у друга партии, и каждая трасса приходила бы в оба приёмника
--- с дырами, поэтому `start`, пока очередь снимает Sentry, отказывает
--- парой, а `start` Sentry с отрезками отказывает при идущем выгрузчике.
---
--- Внешние зависимости: монотонные часы, имя инстанса, признак Sentry,
--- снимающего очередь, и загруженные модули очередей. Настройки, клиент,
--- ждущая партия и счётчики — на модуль: загруженный второй раз модуль
--- считает с нуля.

local fiber = require('fiber')
local json = require('json')

local compress = require('tnt.compress')
local http = require('tnt.http')
local loop = require('tnt.loop')
local monotonic_clock = require('tnt.clock')
local fail = require('tnt.must.fail')
local external = require('tnt.external')
local trace = require('tnt.trace')

local encode = require('tnt.trace.otlp.encode')
local options = require('tnt.trace.otlp.settings')
local reply = require('tnt.trace.otlp.reply')

local journal = require('tnt.log')

--- Журнал с подавлением повторов: такт, падающий на каждом обороте,
--- иначе писал бы по строке в секунду.
local log = journal.changes('tnt.trace.otlp')

--- Фасад пакета.
---@class TntTraceOtlp
local Module = {}

--- Части: доступны тем, кто собирает своё поведение.
Module.encode = encode
Module.reply = reply

--- Умолчания и имена настроек живут у `tnt.trace.otlp.settings`;
--- фасад отдаёт их под прежними именами.
Module.DEFAULT_ENDPOINT = options.DEFAULT_ENDPOINT
Module.DEFAULT_BATCH = options.DEFAULT_BATCH
Module.DEFAULT_INTERVAL = options.DEFAULT_INTERVAL
Module.DEFAULT_TIMEOUT = options.DEFAULT_TIMEOUT
Module.DEFAULT_RETRIES = options.DEFAULT_RETRIES
Module.GZIP = options.GZIP
Module.NONE = options.NONE
Module.COMPRESSIONS = options.COMPRESSIONS
Module.DEFAULT_COMPRESSION = options.DEFAULT_COMPRESSION
Module.SERVICE_NAME = options.SERVICE_NAME

--- Путь трасс у коллектора.
Module.PATH = '/v1/traces'

--- Потолок ожидания перед повтором, секунды: и отступа, и `Retry-After`.
Module.MAX_BACKOFF = 30

--- Уровень gzip — третий.
---
--- Сжатие — счёт в потоке событий узла. Уровни с первого по третий —
--- быстрый путь zlib, и стоят они одинаково: партия в 512 отрезков
--- (230 КБ JSON) ужимается за 0,3 мс, на третьем — до 11,4 %, на первом —
--- до 12,1 %. С четвёртого zlib ищет совпадения лениво, и работа растёт
--- вдвое: шестой уровень — 0,8 мс за 9,9 % (замер на 3.8,
--- `docs/trace-otlp.md`). Кодирование той же партии в JSON стоит 2,7 мс.
Module.GZIP_LEVEL = 3

--- Имя файбера цикла выгрузки.
Module.FIBER = 'trace_otlp'

--- Атрибут ресурса с именем инстанса.
Module.SERVICE_INSTANCE = 'service.instance.id'

--- Чем отказывает `flush`, пока выгрузка не запущена.
Module.NOT_STARTED = 'выгрузка трасс не запущена'

--- Чем отказывает `start`, пока очередь отрезков снимает Sentry.
Module.RIVAL = 'очередь отрезков tnt-trace уже снимает tnt-sentry: '
    .. 'остановите его либо перезапустите Sentry без отрезков (spans = false)'

--- Почему партия выброшена: коллектор отказал так, что повтор не поможет.
Module.REFUSED = 'refused'

--- Почему партия выброшена: повторы кончились.
Module.EXHAUSTED = 'exhausted'

--- Почему партия выброшена: сервер просит ждать дольше потолка.
Module.TOO_LONG = 'too_long'

local source = external.install(Module, {
    monotonic = monotonic_clock.monotonic,

    instance = function()
        local cfg = box.cfg

        -- До настройки `box.cfg` — функция, а не таблица.
        if type(cfg) ~= 'table' then
            return nil
        end

        return cfg.instance_name
    end,

    -- Sentry спрашивается по имени и только загруженный: зависимости
    -- от него нет, а незагруженный очередь не снимает. Признак — его
    -- собственный, а не запись трассы: запись включает и этот выгрузчик,
    -- и тот, кто настраивает трассу, а Sentry без отрезков очереди
    -- не трогает.
    rival = function()
        local sentry = package.loaded['tnt.sentry'] --[[@as any]]

        return sentry ~= nil and sentry.status().takes_spans
    end,

    -- Очереди договора спрашиваются так же — по имени и только
    -- загруженные: зависимости от них у выгрузчика нет.
    module = function(name)
        return package.loaded[name]
    end,
})

--- Действующие настройки; пусто до первого старта.
---@type table|nil
local settings = nil

--- Клиент к коллектору; пусто, пока выгрузка не запущена.
---@type TntHttpClient|nil
local client = nil

--- Атрибуты ресурса, собранные при старте.
---@type table<string, any>
local resource = {}

---@class TntTraceOtlpBatch Партия на отправке
---@field records TntTraceRecord[] Отрезки
---@field attempt integer Номер попытки, с единицы
---@field retry_at number|nil Миг монотонных часов, раньше которого повтор не идёт

--- Партия, ждущая повтора; пусто, когда ждать нечего.
---@type TntTraceOtlpBatch|nil
local pending = nil

--- Счётчики отрезков и партий.
local counted = {
    spans = { accepted = 0, rejected = 0, discarded = 0 },
    batches = { sent = 0, retried = 0, discarded = 0 },
}

--- Чем кончилась последняя неудача; пусто, пока неудач не было.
---@type string|nil
local last_error = nil

--- Идёт ли отправка прямо сейчас.
local sending = false

--- Будит тех, кто ждёт конца чужой отправки.
local idle = fiber.cond()

--- Цикл выгрузки. Собирается внизу, рядом с тем, что его запускает.
---@type TntLoop
local shipper

--- Будильник очереди: набравшаяся партия будит цикл, не дожидаясь такта.
---
--- Толчок цикла управления не уступает — очередь зовёт будильник
--- из закрытия отрезка в чужом файбере, — а только отменяет паузу.
--- Остановленный цикл толчок не будит, поэтому `stop` будильник не снимает.
local function wake()
    shipper:wake()
end

--- Отступ перед повтором: степень двойки от номера неудачной попытки,
--- срезанная потолком.
---@param attempt integer
---@return number
local function backoff(attempt)
    return math.min(2 ^ (attempt - 1), Module.MAX_BACKOFF)
end

--- Что записать об исходе отправки: без адреса и без тела партии.
---@param batch TntTraceOtlpBatch
---@param verdict TntTraceOtlpVerdict
---@return table
local function fields_of(batch, verdict)
    return { spans = #batch.records, attempt = batch.attempt, status = verdict.status, err = verdict.reason }
end

--- Партия отложена на повтор: срок отсчитан отсюда.
---@param batch TntTraceOtlpBatch
---@param verdict TntTraceOtlpVerdict
---@param delay number Сколько ждать, секунды
---@return table outcome
local function postponed(batch, verdict, delay)
    log.warn('партия отрезков не принята, повтор', fields_of(batch, verdict))

    -- Миг — по настоящим часам: в ожидание он не уходит, а сверяется
    -- на такте с теми же часами.
    batch.retry_at = source().monotonic() + delay
    batch.attempt = batch.attempt + 1
    pending = batch
    counted.batches.retried = counted.batches.retried + 1

    return { spans = #batch.records, ok = false, status = verdict.status, retry_in = delay, err = verdict.reason }
end

--- Партия выброшена: коллектор её не примет, повторы кончились либо
--- сервер просит ждать дольше потолка.
---@param batch TntTraceOtlpBatch
---@param verdict TntTraceOtlpVerdict
---@param cause string Почему: `refused`, `exhausted` либо `too_long`
---@return table outcome
local function discarded(batch, verdict, cause)
    pending = nil
    counted.batches.discarded = counted.batches.discarded + 1
    counted.spans.discarded = counted.spans.discarded + #batch.records

    local fields = fields_of(batch, verdict)

    fields.cause = cause
    log.warn('партия отрезков выброшена', fields)

    return { spans = #batch.records, ok = false, status = verdict.status, err = verdict.reason, cause = cause }
end

--- Партия принята; отвергнутые коллектором считаются и называются.
---
--- Отвергнутых не бывает больше, чем отправлено: число сверх партии —
--- ошибка коллектора, и принятых оно в минус не уводит.
---@param batch TntTraceOtlpBatch
---@param verdict TntTraceOtlpVerdict
---@return table outcome
local function accepted(batch, verdict)
    local rejected = math.min(verdict.rejected, #batch.records)

    pending = nil
    counted.spans.accepted = counted.spans.accepted + #batch.records - rejected
    counted.spans.rejected = counted.spans.rejected + rejected

    if rejected > 0 then
        log.warn(
            'коллектор отверг часть отрезков',
            { rejected = rejected, err = verdict.reason }
        )
    end

    return { spans = #batch.records, ok = true, status = verdict.status, rejected = rejected }
end

--- Запрос партии: тело JSON и его заголовки; сжатое — gzip.
---
--- JSON собирается здесь, а не аргументом `json` клиента: сжимается уже
--- готовая строка. Бросить `json.encode` нечем — кодировщик отдаёт
--- только таблицы, строки, конечные числа и `boolean`. Каждая попытка
--- собирает тело заново, и повтор уходит тем же до байта: и кодировщик,
--- и zlib на одном входе дают один выход.
---@param batch TntTraceOtlpBatch
---@return table request Настройки обращения `tnt-http`: `body`, `headers`
local function request_of(batch)
    ---@cast settings table
    local body = json.encode(encode.request(batch.records, resource))
    local headers = { ['content-type'] = http.body.JSON_TYPE }

    if settings.compression == Module.GZIP then
        body = compress.deflate(body, { level = Module.GZIP_LEVEL })
        headers['content-encoding'] = Module.GZIP
    end

    return { body = body, headers = headers }
end

--- Шлёт партию коллектору и решает её судьбу по ответу.
---@param target TntHttpClient Клиент, которым начата отправка
---@param batch TntTraceOtlpBatch
---@return table outcome
local function shipped(target, batch)
    counted.batches.sent = counted.batches.sent + 1

    local verdict = reply.judge(target:post(Module.PATH, request_of(batch)))

    if verdict.verdict == reply.ACCEPTED then
        return accepted(batch, verdict)
    end

    last_error = verdict.reason

    if verdict.verdict == reply.DISCARD then
        return discarded(batch, verdict, Module.REFUSED)
    end

    ---@cast settings table
    if batch.attempt > settings.retries then
        return discarded(batch, verdict, Module.EXHAUSTED)
    end

    local delay = backoff(batch.attempt)
    local asked = verdict.delay

    if asked ~= nil then
        if asked > Module.MAX_BACKOFF then
            return discarded(batch, verdict, Module.TOO_LONG)
        end

        -- Названный сервером срок — нижняя граница, как у `tnt.retry`:
        -- отступ по степени двойки он не сокращает.
        delay = math.max(delay, asked)
    end

    return postponed(batch, verdict, delay)
end

--- Отправляет накопленное тем клиентом, которым отправка начата.
---@param target TntHttpClient
---@return table[] outcomes
local function drained(target)
    ---@cast settings table
    local limit = settings.batch
    local outcomes = {}

    if pending ~= nil then
        if source().monotonic() < pending.retry_at then
            return outcomes
        end

        table.insert(outcomes, shipped(target, pending))

        if pending ~= nil then
            return outcomes
        end
    end

    -- Остановленная или перенастроенная посреди отправки выгрузка
    -- следующей партии не берёт: клиент, которым она начата, уже не тот.
    while client == target do
        local records = trace.take(limit)

        if #records == 0 then
            return outcomes
        end

        table.insert(outcomes, shipped(target, { records = records, attempt = 1 }))

        -- Неполная партия — очередь исчерпана; ждущая — коллектор не принял.
        if #records < limit or pending ~= nil then
            return outcomes
        end
    end

    return outcomes
end

--- Отправляет всё, что накопилось: ждущую партию, если её срок пришёл,
--- и партии из очереди — полные подряд, неполную последней.
---
--- Зовётся тактом цикла; годится и руками — перед остановкой узла.
--- Идущую отправку ждёт до конца: вторая, пущенная рядом, поделила бы
--- с первой ждущую партию. Отказ коллектора — не отказ вызова: партия
--- остаётся ждать повтора, и об этом говорит исход.
---@return table[]|nil outcomes Исход каждой отправленной партии по порядку
---@return string|nil err Выгрузка не запущена
function Module.flush()
    while sending do
        idle:wait()
    end

    -- Клиент читается после ожидания: пока шла чужая отправка,
    -- выгрузку могли остановить.
    local target = client

    if target == nil then
        return nil, Module.NOT_STARTED
    end

    sending = true

    local ok, outcomes = pcall(drained, target)

    sending = false
    idle:broadcast()

    -- Брошенное внутри — ошибка программиста: пробрасывается тем же
    -- значением, без приписки места, но только после того, как ждущие
    -- отпущены. `assert` здесь не годится: он приписывает строке место,
    -- поэтому бросает общий помощник `tnt-must`.
    if not ok then
        fail.raise(outcomes)
    end

    return outcomes
end

--- Атрибуты ресурса: имя инстанса узла, заданное сверх и имя службы.
---@param given table Проверенные настройки
---@return table<string, any>
local function resource_of(given)
    local attributes = { [Module.SERVICE_INSTANCE] = source().instance() }

    for name, value in pairs(given.resource) do
        attributes[name] = value
    end

    attributes[Module.SERVICE_NAME] = given.service_name

    return attributes
end

--- Запускает выгрузку: клиент, крюки трассы, запись отрезков,
--- будильник очереди, цикл.
---
--- Повторный вызов перенастраивает: прежний цикл гасится, клиент
--- собирается заново, ждущая партия и очередь остаются. Негодная форма
--- настроек — исключение на строке вызывающего; очередь отрезков, которую
--- снимает Sentry, и клиент, который не собрался (негодный адрес,
--- ключ без сертификата, заголовок, который ставит выгрузчик, gzip на узле
--- без zlib), — отказ парой, и идущая выгрузка остаётся как была.
---@param opts TntTraceOtlpOptions
---@return boolean|nil ok
---@return string|nil err
function Module.start(opts)
    local given = options.check(opts)

    if source().rival() then
        return nil, Module.RIVAL
    end

    local built, err = options.client(given)

    if built == nil then
        return nil, ('клиент к коллектору не собран: %s'):format(err)
    end

    Module.stop()

    settings = given
    client = built
    resource = resource_of(given)

    -- Крюки — до записи: `trace.record(true)` при загруженном модуле
    -- без крюка пишет предупреждение, а здесь крюки есть. Очередь,
    -- загруженная после старта, крюк получит следующим стартом — ядро
    -- приложения зовёт его на каждом применении настроек — либо
    -- `trace.install` самого приложения.
    trace.install(trace.present(http, source().module))
    trace.record(true)

    -- Порог — партия: узел, рождающий больше потолка очереди за такт,
    -- без будильника терял бы лишнее при свободном коллекторе.
    trace.on_filled(given.batch, wake)

    -- Запись — до цикла: первый такт идёт сразу и уступает управление
    -- на отправке, и запись о старте иначе легла бы после его исхода.
    log.info('выгрузка трасс запущена', {
        endpoint = journal.scrub(given.endpoint),
        service_name = given.service_name,
        batch = given.batch,
        compression = given.compression,
    })

    shipper:set_interval(given.interval)
    shipper:start()

    return true
end

--- Останавливает цикл и выключает запись отрезков; крюки остаются —
--- вне записи они не собирают записей, — и будильник очереди тоже:
--- остановленный цикл толчок не будит.
---
--- Накопленное не шлётся: остановка не ждёт коллектора. Кому нужно
--- дослать, зовёт `flush` до `stop`. Партия, которая уже в пути,
--- доходит; ждущая остаётся до следующего `start`.
function Module.stop()
    if not shipper:running() then
        return
    end

    shipper:stop()
    trace.record(false)
    client = nil

    log.info('выгрузка трасс остановлена')
end

---@class TntTraceOtlpStatus
---@field running boolean Идёт ли цикл выгрузки
---@field recording boolean Складывает ли `tnt-trace` отрезки в очередь
---@field sending boolean Идёт ли отправка прямо сейчас
---@field endpoint string|nil Адрес коллектора без учётных данных
---@field service_name string|nil Имя службы
---@field batch integer|nil Отрезков в партии
---@field interval number|nil Период такта
---@field retries integer|nil Повторов партии сверх первой попытки
---@field compression string|nil Сжатие тела партии: `gzip` либо `none`
---@field spans table<string, integer> Отрезков: `accepted` принято, `rejected` отвергнуто, `discarded` выброшено
---@field batches table<string, integer> Партий: `sent` отправлено, `retried` отложено на повтор, `discarded` выброшено
---@field pending { spans: integer, attempt: integer, retry_at: number|nil }|nil Ждущая партия
---@field last_error string|nil Чем кончилась последняя неудача

--- Состояние: цикл, запись, настройки, счётчики, ждущая партия.
---
--- Тайн здесь нет: учётные данные из адреса вырезаны, заголовки
--- не показываются. Запись видна рядом с циклом: `trace.configure`
--- без `record` выключает её, и идущий цикл без неё выгружает пустоту.
---@return TntTraceOtlpStatus
function Module.status()
    ---@type TntTraceOtlpStatus
    local status = {
        running = shipper:running(),
        recording = trace.status().recording,
        sending = sending,
        spans = table.copy(counted.spans),
        batches = table.copy(counted.batches),
        last_error = last_error,
    }

    if settings ~= nil then
        status.endpoint = journal.scrub(settings.endpoint)
        status.service_name = settings.service_name
        status.batch = settings.batch
        status.interval = settings.interval
        status.retries = settings.retries
        status.compression = settings.compression
    end

    if pending ~= nil then
        status.pending = { spans = #pending.records, attempt = pending.attempt, retry_at = pending.retry_at }
    end

    return status
end

shipper = loop.new({
    name = Module.FIBER,
    interval = Module.DEFAULT_INTERVAL,
    tick = function()
        Module.flush()
    end,
    on_error = function(err)
        log.warn('такт выгрузки трасс не отработал', { err = err })
    end,
})

return Module
