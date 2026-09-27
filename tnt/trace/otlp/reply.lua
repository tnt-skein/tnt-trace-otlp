--- Приговор ответу коллектора — по правилам OTLP/HTTP.
---
--- Коллектор отвечает на партию одним из трёх способов, и у каждого
--- своё продолжение:
---
--- * **принята** — код 2xx; в теле бывает `partialSuccess` с числом
---   отвергнутых отрезков и причиной — такие считаются, но не повторяются:
---   коллектор их уже отверг осознанно;
--- * **повторить** — 429, 502, 503, 504 либо ответа нет вовсе (сеть, срок):
---   ту же партию шлют снова, и если сервер назвал в `Retry-After` число
---   секунд, приговор его несёт;
--- * **выбросить** — всякий другой код: 400 значит, что партия негодна,
---   и OTLP запрещает её повторять; прочие 4xx и 5xx — так же, потому что
---   ни время, ни повтор их не лечат.
---
--- `Retry-After` читает тот же судья, что у `tnt.retry` и `tnt-http`, —
--- только число секунд: дата меряется чужими часами, а они расходятся
--- с нашими. Второго правила для одного заголовка не заводится: выгрузчик
--- и клиент понимали бы одну просьбу сервера по-разному.
---
--- Модуль не знает ни клиента, ни очереди: на входе ответ `tnt-http` либо
--- его отказ, на выходе приговор таблицей. Проверяется таблицей векторов.

local body_of = require('tnt.http.body')
local classify = require('tnt.retry.classify')
local response_of = require('tnt.http.response')

local Module = {}

--- Приговор: партия принята.
Module.ACCEPTED = 'accepted'

--- Приговор: партию повторить.
Module.RETRY = 'retry'

--- Приговор: партию выбросить.
Module.DISCARD = 'discard'

--- Коды, на которые OTLP велит повторять ту же партию.
Module.RETRIABLE = { [429] = true, [502] = true, [503] = true, [504] = true }

--- Чем называется отказ отвергнуть отрезки без объяснений.
Module.NO_REASON = 'коллектор не назвал причины'

---@class TntTraceOtlpVerdict Приговор ответу коллектора
---@field verdict string `accepted`, `retry` либо `discard`
---@field status integer|nil Код ответа, если ответ был
---@field rejected integer Сколько отрезков коллектор отверг из принятой партии
---@field delay number|nil Сколько секунд просил подождать сервер
---@field reason string|nil Чем отказал: для журнала, без адреса

--- Сколько отрезков отвергнуто и почему — из `partialSuccess`.
---
--- Число в ProtoJSON приходит строкой (int64), а в ответе коллектора
--- на удачную партию — пустым объектом; оба читаются одинаково.
--- Считается только целое больше нуля: дробь, отрицательное и NaN
--- коллектор в виду не имел, и счётчик они испортили бы.
---@param body string
---@return integer rejected
---@return string|nil reason
local function partial_of(body)
    local parsed = body_of.parse(body)

    if type(parsed) ~= 'table' or type(parsed.partialSuccess) ~= 'table' then
        return 0, nil
    end

    local partial = parsed.partialSuccess
    local rejected = tonumber(partial.rejectedSpans)

    if rejected == nil or rejected < 1 or rejected ~= math.floor(rejected) then
        return 0, nil
    end

    local message = partial.errorMessage

    if type(message) ~= 'string' or message == '' then
        message = Module.NO_REASON
    end

    ---@cast rejected integer
    return rejected, message
end

--- Приговор ответу.
---@param answer TntHttpResponse|nil Ответ коллектора
---@param failure TntHttpFailure|nil Отказ клиента, если ответа нет
---@return TntTraceOtlpVerdict
function Module.judge(answer, failure)
    if answer == nil then
        ---@cast failure TntHttpFailure
        -- Причина без адреса: в адресе коллектора бывают ключи доступа.
        return { verdict = Module.RETRY, rejected = 0, reason = failure.reason or tostring(failure) }
    end

    if response_of.ok(answer.status) then
        local rejected, reason = partial_of(answer.body)

        return { verdict = Module.ACCEPTED, status = answer.status, rejected = rejected, reason = reason }
    end

    local reason = ('ответ %d %s; %s'):format(answer.status, answer.reason, body_of.fragment(answer.body))

    if Module.RETRIABLE[answer.status] then
        return {
            verdict = Module.RETRY,
            status = answer.status,
            rejected = 0,
            delay = classify.delay_of({ retry_after = answer.headers['retry-after'] }),
            reason = reason,
        }
    end

    return { verdict = Module.DISCARD, status = answer.status, rejected = 0, reason = reason }
end

return Module
