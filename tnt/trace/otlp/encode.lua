--- Кодировщик OTLP/HTTP JSON: записи закрытых отрезков `tnt-trace` —
--- в тело `ExportTraceServiceRequest`.
---
--- JSON, а не protobuf: коллектор принимает оба тем же путём, а бинарный
--- protobuf на чистом Lua — отдельный кодировщик на сотни строк со своими
--- проверками. Правила записи — ProtoJSON, и три из них коллектор
--- проверяет строго:
---
--- * `traceId` и `spanId` — **шестнадцатеричной строкой**, а не base64,
---   как велит общий ProtoJSON для `bytes`: на base64 коллектор отвечает
---   400 «length mismatch»;
--- * `startTimeUnixNano` и `endTimeUnixNano` — десятичной **строкой**:
---   int64 в double теряет наносекунды выше 2⁵³;
--- * `kind` и `status.code` — целыми по перечислениям OTLP.
---
--- Тайны в атрибутах вырезаются правилом `tnt-log` перед отправкой:
--- атрибут с именем тайны уходит как `[скрыто]`, строки проходят
--- `log.scrub`. Класть тайны в атрибуты не надо и так, но граница
--- процесса — последнее место, где их можно остановить.
---
--- Число-атрибут — `intValue` (строкой, как int64 в ProtoJSON), если оно
--- целое и умещается в int64, иначе `doubleValue`. NaN и бесконечности
--- уходят строкой: `json.encode` пишет их литералами `nan` и `inf`,
--- которые не JSON, и коллектор отверг бы всю партию.
---
--- Модуль не знает ни клиента, ни очереди: он про таблицы, и проверяется
--- эталоном.

local log = require('tnt.log')

local Module = {}

--- Имя области инструментирования: кто рождает отрезки.
Module.SCOPE = 'tnt-trace'

--- Род отрезка — в число OTLP (`SpanKind`).
Module.KIND = { internal = 1, server = 2, client = 3, producer = 4, consumer = 5 }

--- Род, которого OTLP не знает: `SPAN_KIND_UNSPECIFIED`.
Module.UNSPECIFIED_KIND = 0

--- Статус отрезка с ошибкой: `STATUS_CODE_ERROR`.
Module.ERROR_CODE = 2

--- Статус записи, у которой исход не оценивали: поле `status` не пишется.
Module.UNSET = 'unset'

--- Граница int64: целое число не меньше её по модулю в `intValue`
--- не умещается, и `%d` напечатал бы его насыщенным до предела.
local INT64_LIMIT = 2 ^ 63

--- Список атрибутов OTLP из таблицы скаляров: ключи по алфавиту.
---
--- Порядок держится ради воспроизводимого тела: одинаковые записи дают
--- одинаковые байты, и эталон в проверках сверяется дословно.
---@param values table<string, any>
---@return table[]|nil Список `{ key, value }`; пусто, если атрибутов нет
function Module.attributes(values)
    local names = {}

    for name in pairs(values) do
        table.insert(names, name)
    end

    -- Пустой список в JSON Tarantool пишет как `[]`, и поле честнее
    -- не писать вовсе: у protobuf отсутствие и пустота — одно.
    if #names == 0 then
        return nil
    end

    table.sort(names)

    local list = {}

    for _, name in ipairs(names) do
        table.insert(list, { key = name, value = Module.value(name, values[name]) })
    end

    return list
end

--- Значение атрибута в виде `AnyValue`.
---
--- Имя решает раньше значения: атрибут с именем тайны прячется целиком,
--- каким бы ни было значение.
---@param name string Имя атрибута — по нему узнаётся тайна
---@param value any
---@return table
function Module.value(name, value)
    if log.secret(name) then
        return { stringValue = log.HIDDEN }
    end

    local kind = type(value)

    if kind == 'string' then
        return { stringValue = log.scrub(value) }
    end

    if kind == 'boolean' then
        return { boolValue = value }
    end

    if kind ~= 'number' then
        return { stringValue = tostring(value) }
    end

    -- NaN не равен себе; бесконечности по модулю не меньше границы,
    -- и `math.floor` их не меняет — обе ветки ниже приняли бы их за целые.
    if value ~= value or math.abs(value) == math.huge then
        return { stringValue = tostring(value) }
    end

    if value == math.floor(value) and math.abs(value) < INT64_LIMIT then
        return { intValue = ('%d'):format(value) }
    end

    return { doubleValue = value }
end

--- Наносекунды — десятичной строкой без суффикса `LL`.
---
--- Стенные часы отдают int64 cdata, и `tostring` пишет его с суффиксом
--- `LL` (`ULL` у беззнакового) — суффикс снимается, остальное и есть
--- знак с цифрами; `%d` cdata не принимает. Число Lua печатается через
--- `%d` — `tostring` дало бы ему экспоненту.
---@param nanos any int64 cdata либо число
---@return string
function Module.nanos(nanos)
    if type(nanos) == 'cdata' then
        return (tostring(nanos):gsub('U?LL$', ''))
    end

    return ('%d'):format(nanos)
end

--- Статус отрезка: только у отрезка с ошибкой.
---
--- У отрезка без оценки исхода статус в OTLP — `UNSET`, то есть
--- умолчание protobuf, и поле не пишется.
---@param record TntTraceRecord
---@return table|nil
local function status_of(record)
    if record.status == Module.UNSET then
        return nil
    end

    return { code = Module.ERROR_CODE, message = log.scrub(record.error) }
end

--- Один отрезок в виде `Span` OTLP.
---@param record TntTraceRecord Запись закрытого отрезка из `trace.take`
---@return table
function Module.span(record)
    return {
        traceId = record.trace_id,
        spanId = record.span_id,
        parentSpanId = record.parent_id,
        traceState = record.tracestate,
        name = log.scrub(record.name),
        kind = Module.KIND[record.kind] or Module.UNSPECIFIED_KIND,
        startTimeUnixNano = Module.nanos(record.started),
        endTimeUnixNano = Module.nanos(record.finished),
        attributes = Module.attributes(record.attributes),
        status = status_of(record),
        -- Младшие восемь бит — флаги W3C как есть: коллектор по ним
        -- видит выборку, не разбирая заголовка.
        flags = tonumber(record.trace_flags, 16),
    }
end

--- Тело запроса `ExportTraceServiceRequest`: один ресурс, одна область,
--- отрезки партии по порядку.
---@param records TntTraceRecord[] Партия из `trace.take`
---@param resource table<string, any> Атрибуты ресурса: `service.name` и прочие
---@return table
function Module.request(records, resource)
    local spans = {}

    for _, record in ipairs(records) do
        table.insert(spans, Module.span(record))
    end

    return {
        resourceSpans = {
            {
                resource = { attributes = Module.attributes(resource) },
                scopeSpans = { { scope = { name = Module.SCOPE }, spans = spans } },
            },
        },
    }
end

return Module
